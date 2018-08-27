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

package support;

import com.hazelcast.core.IMap;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.List;

import static javax.swing.WindowConstants.EXIT_ON_CLOSE;

/**
 * Displays a live time graph based on the data it fetches from a
 * Hazelcast map.
 */
public class SystemMonitorGui {
    private static final int WINDOW_X = 100;
    private static final int WINDOW_Y = 100;
    private static final int WINDOW_WIDTH = 1000;
    private static final int WINDOW_HEIGHT = 650;
    private static final int POLL_INTERVAL = 25;
    private static final int SCALE_X = 25;
    private static final int SCALE_Y = 100;

    private final IMap<Long, Double> eventSource;
    private final List<Point> histogram = new ArrayList<>();
    private long initialTimestamp;
    private JPanel mainPanel;

    public SystemMonitorGui(IMap<Long, Double> eventSource) {
        this.eventSource = eventSource;
        EventQueue.invokeLater(this::buildFrame);
    }

    private void buildFrame() {
        final JFrame frame = new JFrame();
        frame.setBackground(Color.WHITE);
        frame.setDefaultCloseOperation(EXIT_ON_CLOSE);
        frame.setTitle("Hazelcast Jet Source Builder Sample");
        frame.setBounds(WINDOW_X, WINDOW_Y, WINDOW_WIDTH, WINDOW_HEIGHT);
        frame.setLayout(new BorderLayout());
        mainPanel = new JPanel() {
            @Override
            protected void paintComponent(Graphics g) {
                super.paintComponent(g);
                g.setColor(Color.BLUE);
                Rectangle bounds = g.getClipBounds();
                int prevX = 0;
                for (Point p : histogram) {
                    if (p.x > bounds.width) {
                        histogram.clear();
                        return;
                    }
                    int barWidth = p.x - prevX;
                    if (p.y > 0) {
                        g.fillRect(prevX, bounds.height / 2 - p.y, barWidth, p.y);
                    } else {
                        g.fillRect(prevX, bounds.height / 2, barWidth, -p.y);
                    }
                    prevX = p.x;
                }
            }
        };
        mainPanel.setBackground(Color.WHITE);
        frame.add(mainPanel);
        frame.setVisible(true);
        new Timer(POLL_INTERVAL, new ActionListener() {
            private long nextTimestampToPoll = System.currentTimeMillis();

            @Override
            public void actionPerformed(ActionEvent e) {
                long now = System.currentTimeMillis();
                if (histogram.isEmpty()) {
                    initialTimestamp = now;
                }
                int prevX = -1;
                while (nextTimestampToPoll <= now) {
                    long ts = nextTimestampToPoll++;
                    Double value = eventSource.remove(ts);
                    int x = (int) ((ts - initialTimestamp) / SCALE_X);
                    if (value != null && x != prevX) {
                        prevX = x;
                        histogram.add(new Point(x, (int) (value / SCALE_Y)));
                        mainPanel.repaint();
                    }
                }
            }
        }).start();
    }
}
