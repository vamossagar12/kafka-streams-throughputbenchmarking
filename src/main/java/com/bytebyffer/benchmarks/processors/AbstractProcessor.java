package com.bytebyffer.benchmarks.processors;

import com.bytebyffer.benchmarks.Metrics;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class AbstractProcessor {

    protected long calcGCTime() {
        GarbageCollectorMXBean[] gcBeans = ManagementFactory.getGarbageCollectorMXBeans().toArray(new GarbageCollectorMXBean[0]);

        long totalDuration = 0;
        for (GarbageCollectorMXBean gcBean : gcBeans) {
            long collectorTime = gcBean.getCollectionTime();

            totalDuration += collectorTime;
        }

        return totalDuration * 1000000L;
    }

    protected long calcGCCount() {
        GarbageCollectorMXBean[] gcBeans = ManagementFactory.getGarbageCollectorMXBeans().toArray(new GarbageCollectorMXBean[0]);

        long totalCount = 0;
        for (GarbageCollectorMXBean gcBean : gcBeans) {
            totalCount += gcBean.getCollectionCount();
        }

        return totalCount;
    }

    private double percentile(List<Double> values, double percentile) {
        Collections.sort(values);
        int index = (int) Math.ceil(percentile / 100.0 * values.size());
        return values.get(index-1);
    }

    protected void printMetrics(List<Metrics> metrics, String operator) {
        double totalTime = metrics.stream().mapToDouble(Metrics::getReal).sum();
        double totalCpuTime = metrics.stream().mapToDouble(Metrics::getCpu).sum();
        double totalGCTime = metrics.stream().mapToDouble(Metrics::getGcTime).sum();
        long totalGC = metrics.stream().mapToLong(Metrics::getGcCount).sum();

        double averageThroughput = metrics.stream().mapToDouble(Metrics::getThroughput).average().orElse(0.0);
        double p95Throughput = percentile(metrics.stream().map(Metrics::getThroughput).collect(Collectors.toList()), 95);
        double p99Throughput = percentile(metrics.stream().map(Metrics::getThroughput).collect(Collectors.toList()), 99);

        System.out.printf("Operator: %s, Real: %.3f CPU: %.3f GC: %.3f GCCount: %d avg throughput: %.3f op/s p95 throughput: %.3f op/s p99 throughput: %.3f op/s %n",
                operator,
                totalTime,
                totalCpuTime,
                totalGCTime,
                totalGC,
                averageThroughput,
                p95Throughput,
                p99Throughput
        );
    }
}
