package com.bytebyffer.benchmarks.processors;


import com.bytebyffer.benchmarks.Metrics;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RangeQueryProcessor extends AbstractProcessor implements Processor<String, String, String, String> {

    private ProcessorContext<String, String> context;
    private KeyValueStore<String, Integer> kvStore;
    private OperatingSystemMXBean operatingSystemMXBean;
    private com.sun.management.OperatingSystemMXBean os;
    private final String key = "key";
    private AtomicInteger counter;
    private List<Metrics> metrics;



    @Override
    public void init(ProcessorContext<String, String> context) {
        Processor.super.init(context);
        // keep the processor context locally because we need it in punctuate() and commit()
        this.context = context;

        // retrieve the key-value store named "Counts"
        kvStore = (KeyValueStore) context.getStateStore("store");

        operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
        os = (com.sun.management.OperatingSystemMXBean) operatingSystemMXBean;
        counter = new AtomicInteger(0);
        metrics = new ArrayList<>();
    }

    @Override
    public void process(Record<String, String> record) {
        long startCPU = os.getProcessCpuTime();
        long startReal = System.nanoTime();
        long startGC = calcGCTime();
        long startGCCount = calcGCCount();

        int numRecords = 0;

        final KeyValueIterator<String, Integer> range = kvStore.range(key + ":0", key + ":999999");

        while (range.hasNext()){
            range.next();
            numRecords++;
        }
        int index = counter.getAndIncrement();
        double timeTaken = (System.nanoTime() - startReal) / 1000000000.0;
        double throughPut = 1_000_000 / timeTaken;
        metrics.add(index, new Metrics(
                timeTaken,
                ((os.getProcessCpuTime() - startCPU) / 1000000000.0),
                ((calcGCTime() - startGC) / 1000000000.0),
                (calcGCCount() - startGCCount),
                throughPut));
        context.forward(record);
    }

    @Override
    public void close() {
        printMetrics(metrics, "range");
    }
}
