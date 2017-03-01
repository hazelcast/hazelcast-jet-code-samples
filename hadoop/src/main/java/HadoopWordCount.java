import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Distributed;
import com.hazelcast.jet.Distributed.Function;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.config.InstanceConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.connector.hadoop.WriteHdfsP;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.KeyExtractors.entryKey;
import static com.hazelcast.jet.KeyExtractors.wholeItem;
import static com.hazelcast.jet.Partitioner.HASH_CODE;
import static com.hazelcast.jet.Processors.flatMap;
import static com.hazelcast.jet.Processors.groupAndAccumulate;
import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.connector.hadoop.ReadHdfsP.readHdfs;
import static java.lang.Runtime.getRuntime;
import static java.util.Comparator.comparingLong;

/**
 * Analyzes a set of documents and finds the number of occurrences of each word
 * they contain. Implemented with the following DAG:
 * <pre>
 *                --------
 *               | source |
 *                --------
 *                    |
 *             (lineId, line)
 *                    V
 *               ----------
 *              | tokenize |
 *               ----------
 *                    |
 *                 (word)
 *                    V
 *                --------
 *               | reduce |
 *                --------
 *                    |
 *            (word, localCount)
 *                    V
 *                ---------
 *               | combine |
 *                ---------
 *                    |
 *           (word, totalCount)
 *                    V
 *                 ------
 *                | sink |
 *                 ------
 * </pre>
 * This is how the DAG works:
 * <ul><li>
 *     In the {@code books} module there are some books in plain text format.
 * </li><li>
 *     {@code source} reads these books from local file system via our HDFS reader.
 *     Books are passed as input path to the configuration, and {@code TextInputFormat}
 *     is used to read the books line by line and emits as {@code (lineId, line)}.
 *
 *     HDFS stores a file by dividing it into blocks and storing each block on different servers.
 *     We ask HDFS to obtain the {@code InputSplit}s which are groups of blocks.
 *     These splits are assigned to members according to locality and overall balance across members
 * </li><li>
 *     Lines are sent over a <em>local</em> edge to the {@code tokenize} vertex.
 *     This means that the tuples stay within the member where they were created
 *     and are routed to locally-running {@code tokenize} processors. Since the
 *     edge isn't partitioned, the choice of processor is arbitrary but fair and
 *     balances the traffic to each processor.
 * </li><li>
 *     {@code tokenize} splits each line into words and emits them.
 * </li><li>
 *     Words are sent over a <em>partitioned local</em> edge which routes
 *     all the items with the same word to the same local {@code reduce}
 *     processor.
 * </li><li>
 *     {@code reduce} collates tuples by word and maintains the count of each
 *     seen word. After having received all the input from {@code tokenize}, it
 *     emits tuples of the form {@code (word, localCount)}.
 * </li><li>
 *     Tuples with local sums are sent to {@code combine} over a <em>distributed
 *     partitioned</em> edge. This means that for each word there will be a single
 *     unique instance of a {@code combine} processor in the whole cluster and
 *     tuples will be sent over the network if needed.
 * </li><li>
 *     {@code combine} combines the partial sums into totals and emits them.
 * </li><li>
 *     Finally, the {@code sink} vertex stores the result in the output
 *     directory via HDFS writer. Each processor creates a file with {@code SequenceFileOutputFormat}
 *     which are named as <em>uuid</em> of the member and the index of the processor.
 *     Since {@code SequenceFileOutputFormat} requires {@code Writable} key/values,
 *     mappers are used to convert {@code String}/{@code Long} to {@code Text}/{@code LongWritable}
 * </li></ul>
 */
public class HadoopWordCount {

    public static void main(String[] args) throws Exception {
        // Delete the output directory
        FileSystem.getLocal(new Configuration()).delete(new Path("hadoop/output"), true);

        JetConfig cfg = new JetConfig();
        cfg.setInstanceConfig(new InstanceConfig().setCooperativeThreadCount(
                Math.max(1, getRuntime().availableProcessors() / 2)));

        JetInstance jetInstance = Jet.newJetInstance(cfg);
        Jet.newJetInstance(cfg);

        try {
            System.out.print("\nCounting words... ");
            long start = System.nanoTime();
            jetInstance.newJob(buildDag()).execute().get();
            System.out.print("done in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + " milliseconds.");
            printResults(jetInstance);
        } finally {
            Jet.shutdownAll();
        }
    }

    private static void printResults(JetInstance jetInstance) throws Exception {
        DAG dag = new DAG();

        JobConf jobConf = new JobConf();
        jobConf.setInputFormat(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(jobConf, new Path("hadoop/output"));
        Vertex source = dag.newVertex("source", readHdfs(jobConf));
        Vertex sort = dag.newVertex("sort", SorterP.sortAndLimit(100)).localParallelism(1);

        dag.edge(between(source, sort)
                .distributed().allToOne());
        jetInstance.newJob(dag).execute().get();
    }

    @Nonnull
    private static DAG buildDag() {
        final Pattern delimiter = Pattern.compile("\\W+");
        final Distributed.Supplier<Long> initialZero = () -> 0L;

        DAG dag = new DAG();
        // nil -> (lineId, line)
        JobConf readJobConf = new JobConf();
        readJobConf.setInputFormat(TextInputFormat.class);
        TextInputFormat.addInputPath(readJobConf, new Path("books/src/main/resources/books"));
        Vertex source = dag.newVertex("source", readHdfs(readJobConf));

        // line -> words
        Vertex tokenize = dag.newVertex("tokenize",
                flatMap((Map.Entry line) -> traverseArray(delimiter.split(line.getValue().toString().toLowerCase()))
                        .filter(word -> !word.isEmpty()))
        );

        // word -> (word, count)
        Vertex reduce = dag.newVertex("reduce",
                groupAndAccumulate(initialZero, (count, x) -> count + 1)
        );
        // (word, count) -> (word, count)
        Vertex combine = dag.newVertex("combine",
                groupAndAccumulate(Map.Entry<String, Long>::getKey, initialZero,
                        (Long count, Map.Entry<String, Long> wordAndCount) -> count + wordAndCount.getValue())
        );
        // (word, count) -> nil
        // configure write job with SequenceFileOutputFormat and output path
        JobConf jobConf = new JobConf();
        jobConf.setOutputFormat(SequenceFileOutputFormat.class);
        jobConf.setOutputCommitter(FileOutputCommitter.class);
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(LongWritable.class);
        SequenceFileOutputFormat.setOutputPath(jobConf, new Path("hadoop/output"));

        Vertex sink = dag.newVertex("sink", WriteHdfsP.writeHdfs(jobConf,
                (Function<String, Text>) Text::new,
                (Function<Long, LongWritable>) LongWritable::new));

        return dag.edge(between(source, tokenize))
                .edge(between(tokenize, reduce)
                        .partitioned(wholeItem(), HASH_CODE))
                .edge(between(reduce, combine)
                        .distributed()
                        .partitioned(entryKey()))
                .edge(between(combine, sink));
    }

    private static class SorterP extends AbstractProcessor {

        private final int limit;
        Map<Text, LongWritable> map = new HashMap<>();

        private SorterP(int limit) {
            this.limit = limit;
        }

        @Override
        protected boolean tryProcess0(@Nonnull Object item) throws Exception {
            Map.Entry<Text, LongWritable> entry = (Map.Entry<Text, LongWritable>) item;
            map.put(entry.getKey(), entry.getValue());
            return true;
        }

        @Override
        public boolean complete() {
            if (map.isEmpty()) {
                return true;
            }
            System.out.format(" Top %d entries are:%n", limit);
            System.out.println("/-------+---------\\");
            System.out.println("| Count | Word    |");
            System.out.println("|-------+---------|");
            map.entrySet().stream()
                    .sorted(comparingLong((Map.Entry<Text, LongWritable> e) -> e.getValue().get()).reversed())
                    .limit(limit)
                    .forEach(e -> System.out.format("|%6d | %-8s|%n", e.getValue().get(), e.getKey().toString()));
            System.out.println("\\-------+---------/");
            return true;
        }

        public static ProcessorSupplier sortAndLimit(int limit) {
            return ProcessorSupplier.of(() -> new SorterP(limit));
        }
    }
}
