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
import com.hazelcast.map.listener.EntryUpdatedListener;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;

import javax.swing.*;
import java.awt.*;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;

import static java.lang.Math.max;
import static javax.swing.WindowConstants.EXIT_ON_CLOSE;

/**
 * Displays a live bar chart of each stock and its current trading volume
 * on the simulated stock exchange.
 */
public class TradingVolumeGui {
    private static final int WINDOW_X = 100;
    private static final int WINDOW_Y = 100;
    private static final int WINDOW_WIDTH = 1200;
    private static final int WINDOW_HEIGHT = 650;
    private static final int INITIAL_TOP_Y = 5_000_000;

    private final IMap<String, Long> hzMap;

    public TradingVolumeGui(IMap<String, Long> hzMap) {
        this.hzMap = hzMap;
        EventQueue.invokeLater(this::startGui);
    }

    private void startGui() {
        DefaultCategoryDataset dataSet = new DefaultCategoryDataset();
        ChartFrame chartFrame = new ChartFrame(dataSet);
        ValueAxis yAxis = chartFrame.getRangeAxis();

        long[] topY = {INITIAL_TOP_Y};
        EntryUpdatedListener<String, Long> addEntryListener = event -> {
            EventQueue.invokeLater(() -> {
                dataSet.addValue(event.getValue(), event.getKey(), "");
                topY[0] = max(topY[0], INITIAL_TOP_Y * (1 + event.getValue() / INITIAL_TOP_Y));
                yAxis.setRange(0, topY[0]);
            });
        };
        String listenerId = hzMap.addEntryListener(addEntryListener, true);
        chartFrame.setShutdownHook(() -> hzMap.removeEntryListener(listenerId));
    }

    private static class ChartFrame {

        private final CategoryPlot plot;
        private final JFrame frame;

        ChartFrame(CategoryDataset dataSet) {
            JFreeChart chart = ChartFactory.createBarChart(
                    "Trading Volume", "Stock", "Volume, USD", dataSet,
                    PlotOrientation.HORIZONTAL, true, true, false);
            plot = chart.getCategoryPlot();
            plot.setBackgroundPaint(Color.WHITE);
            plot.setDomainGridlinePaint(Color.DARK_GRAY);
            plot.setRangeGridlinePaint(Color.DARK_GRAY);
            plot.getRenderer().setSeriesPaint(0, Color.BLUE);

            frame = new JFrame();
            frame.setBackground(Color.WHITE);
            frame.setDefaultCloseOperation(EXIT_ON_CLOSE);
            frame.setTitle("Hazelcast Jet Source Builder Sample");
            frame.setBounds(WINDOW_X, WINDOW_Y, WINDOW_WIDTH, WINDOW_HEIGHT);
            frame.setLayout(new BorderLayout());
            frame.add(new ChartPanel(chart));
            frame.setVisible(true);
        }

        ValueAxis getRangeAxis() {
            return plot.getRangeAxis();
        }

        void setShutdownHook(Runnable runnable) {
            frame.addWindowListener(
                    new WindowAdapter() {
                        @Override
                        public void windowClosing(WindowEvent windowEvent) {
                            runnable.run();
                        }
                    }
            );
        }
    }
}
