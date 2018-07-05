/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import org.h2.tools.DeleteDbFiles;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * Demonstrates dumping a map's values to a table.
 */
public class JdbcSink {

    private static final String DIR = "~";
    private static final String DB = JdbcSink.class.getSimpleName();
    private static final String DB_CONNECTION_URL = "jdbc:h2:" + DIR + "/" + DB;
    private static final String MAP_NAME = "userMap";
    private static final String TABLE_NAME = "USER_TABLE";

    private JetInstance jet;

    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();

        p.drawFrom(Sources.<Integer, User>map(MAP_NAME))
         .map(Map.Entry::getValue)
         .drainTo(Sinks.jdbc("INSERT INTO " + TABLE_NAME + "(id, name) VALUES(?, ?)",
                 DB_CONNECTION_URL,
                 (stmt, user) -> {
                     stmt.setInt(1, user.getId());
                     stmt.setString(2, user.getName());
                 }));
        return p;
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        new JdbcSink().go();
    }

    private void go() throws SQLException {
        try {
            setup();
            jet.newJob(buildPipeline()).join();
            printTable();
        } finally {
            Jet.shutdownAll();
        }
    }

    private void setup() throws SQLException {
        createTable();

        jet = Jet.newJetInstance();
        Jet.newJetInstance();

        IMapJet<Integer, User> map = jet.getMap(MAP_NAME);
        IntStream.range(0, 100).forEach(i -> map.put(i, new User(i, "name-" + i)));
    }

    private void createTable() throws SQLException {
        DeleteDbFiles.execute(DIR, DB, true);
        try (
                Connection connection = DriverManager.getConnection(DB_CONNECTION_URL);
                Statement statement = connection.createStatement()
        ) {
            statement.execute("CREATE TABLE " + TABLE_NAME + "(id int primary key, name varchar(255))");
        }
    }

    private void printTable() throws SQLException {
        try (
                Connection connection = DriverManager.getConnection(DB_CONNECTION_URL);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT * FROM " + TABLE_NAME)
        ) {
            while (resultSet.next()) {
                System.out.println(new User(resultSet.getInt(1), resultSet.getString(2)));
            }
        }
    }
}
