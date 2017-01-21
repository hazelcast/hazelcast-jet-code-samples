/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.Distributed.Optional;
import com.hazelcast.jet.stream.IStreamMap;

import javax.swing.*;
import java.awt.*;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

import static java.util.Collections.emptyList;
import static java.util.Comparator.comparingDouble;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.reducing;
import static javax.swing.WindowConstants.EXIT_ON_CLOSE;

class SearchGui {
    private static final int WINDOW_X = 200;
    private static final int WINDOW_Y = 200;
    private static final int WINDOW_WIDTH = 300;
    private static final int WINDOW_HEIGHT = 250;

    private final IStreamMap<Long, String> docId2Name;
    private final IStreamMap<String, List<Entry<Long, Double>>> tfidfIndex;

    SearchGui(IStreamMap<Long, String> docId2Name, IStreamMap<String, List<Entry<Long, Double>>> tfidfIndex) {
        this.docId2Name = docId2Name;
        this.tfidfIndex = tfidfIndex;
        SwingUtilities.invokeLater(this::buildFrame);
    }

    private void buildFrame() {
        final JFrame frame = new JFrame();
        frame.setBackground(Color.WHITE);
        frame.setDefaultCloseOperation(EXIT_ON_CLOSE);
        frame.setTitle("Hazelcast Jet TF-IDF");
        frame.setBounds(WINDOW_X, WINDOW_Y, WINDOW_WIDTH, WINDOW_HEIGHT);
        frame.setLayout(new BorderLayout());
        final JPanel mainPanel = new JPanel();
        mainPanel.setLayout(new BorderLayout(10, 10));
        frame.add(mainPanel);
        final JTextField input = new JTextField();
        mainPanel.add(input, BorderLayout.NORTH);
        final JTextArea output = new JTextArea();
        mainPanel.add(output, BorderLayout.CENTER);
        input.addActionListener(e -> output.setText(search(input.getText().split("\\s+"))));
        frame.setVisible(true);
    }

    private String search(String... terms) {
        return Arrays.stream(terms)
                     .map(String::toLowerCase)
                     // retrieve all (docId, score) entries from the index
                     .flatMap(term -> Optional.ofNullable(tfidfIndex.get(term))
                                              .orElse(emptyList())
                                              .stream())
                     // group by docId, accumulate the number of terms found in the document
                     // and the total TF-IDF score of the document
                     .collect(groupingBy(Entry<Long, Double>::getKey, reducing(
                             new SimpleImmutableEntry<>(0L, 0.0),
                             (acc, docScore) -> new SimpleImmutableEntry<>(
                                     acc.getKey() + 1, acc.getValue() + docScore.getValue()))))
                     .entrySet().stream()
                     // filter out documents which don't contain all the entered terms
                     .filter((Entry<Long, Entry<Long, Double>> e) -> e.getValue().getKey() == terms.length)
                     // sort documents by score, descending
                     .sorted(comparingDouble(
                             (Entry<Long, Entry<Long, Double>> e) -> e.getValue().getValue()).reversed())
                     .map(e -> String.format("%5.2f %s",
                             e.getValue().getValue() / terms.length, docId2Name.get(e.getKey())))
                     .collect(joining("\n"));
    }
}
