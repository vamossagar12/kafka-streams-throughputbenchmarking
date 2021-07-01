package com.bytebyffer.benchmarks;

import com.bytebyffer.benchmarks.processors.PutAllProcessor;
import com.bytebyffer.benchmarks.processors.RangeQueryProcessor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ByteBufferBenchmarkingTopologyBuilder {

    public static void main(String[] args) throws InterruptedException, StreamsException {

        Topology topology = new Topology();

        StoreBuilder<KeyValueStore<String, Integer>> countStoreSupplier = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("store"),
                Serdes.String(),
                Serdes.Integer())
                .withCachingDisabled()
                .withLoggingDisabled();

        // add the source processor node that takes Kafka topic "source-topic" as input
        topology.addSource("Source", "source-topic")
                .addProcessor("putAll", PutAllProcessor::new, "Source")
                .addProcessor("range", RangeQueryProcessor::new, "putAll")
                .addStateStore(countStoreSupplier, "putAll")
                .addStateStore(countStoreSupplier, "range")
                // add the sink processor node that takes Kafka topic "sink-topic" as output
                .addSink("Sink", "sink-topic", "range");
        System.out.println(topology.describe());

        KafkaStreams kafkaStreams = new KafkaStreams(topology, getProperties());
        kafkaStreams.cleanUp();

        CountDownLatch waitLatch = new CountDownLatch(1);

        kafkaStreams.setUncaughtExceptionHandler(e -> {
            e.printStackTrace(System.err);
            waitLatch.countDown();
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        });

        Runtime.getRuntime().addShutdownHook(new Thread("kafka-streams-bytebuffer-benchmarking-shutdown-hook") {
            @Override
            public void run() {
                waitLatch.countDown();
                kafkaStreams.close();
            }
        });

        kafkaStreams.start();
        waitLatch.await();
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "bytebuffer-benchmarking-client-original");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "bytebuffer-original");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/data/state-store-original");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return props;
    }
}
