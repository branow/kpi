package com.npp.util;

import org.apache.kafka.streams.kstream.ForeachAction;

public class ThroughputMonitor<K, V> implements ForeachAction<K, V> {
    private long count = 0;
    private long startTime = System.currentTimeMillis();
    private final String stageName;
    private final int logBatchSize;

    public ThroughputMonitor(String stageName, int logBatchSize) {
        this.stageName = stageName;
        this.logBatchSize = logBatchSize;
    }

    @Override
    public void apply(K key, V value) {
        count++;
        if (count % logBatchSize == 0) {
            long now = System.currentTimeMillis();
            long duration = now - startTime;
            double rate = (double) logBatchSize / (duration / 1000.0);
            System.out.printf("[%s] Processed %d records. Last batch: %.2f records/sec%n", 
                stageName, count, rate);
            startTime = now; // Reset timer for next batch
        }
    }
}
