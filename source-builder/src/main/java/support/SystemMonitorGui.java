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
import com.hazelcast.map.listener.EntryAddedListener;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import javax.swing.*;
import java.awt.*;

import static java.awt.EventQueue.invokeLater;
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
    private static final int SCALE_Y = 1024;

    private final IMap<Long, Double> eventSource;

    public SystemMonitorGui(IMap<Long, Double> eventSource) {
        this.eventSource = eventSource;
        invokeLater(this::buildFrame);
    }

    private void buildFrame() {
        final JFrame frame = new JFrame();
        frame.setBackground(Color.WHITE);
        frame.setDefaultCloseOperation(EXIT_ON_CLOSE);
        frame.setTitle("Hazelcast Jet Source Builder Sample");
        frame.setBounds(WINDOW_X, WINDOW_Y, WINDOW_WIDTH, WINDOW_HEIGHT);
        frame.setLayout(new BorderLayout());

        XYSeriesCollection dataSet = new XYSeriesCollection();
        XYSeries series = new XYSeries("Rate", false);
        dataSet.addSeries(series);

        JFreeChart xyChart = ChartFactory.createXYLineChart(
                "Memory Allocation Rate",
                "Time (ms)", "Allocation Rate (KB/s)",
                dataSet,
                PlotOrientation.VERTICAL,
                true, true, false);
        frame.add(new ChartPanel(xyChart));
        frame.setVisible(true);

        long initialTimestamp = System.currentTimeMillis();
        eventSource.addEntryListener((EntryAddedListener<Long, Double>) event -> {
            long x = event.getKey() - initialTimestamp;
            double y = event.getValue() / SCALE_Y;
            invokeLater(() -> series.add(x, y));
            eventSource.remove(event.getKey());
        }, true);
    }
}
