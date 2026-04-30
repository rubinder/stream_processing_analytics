package com.example.common;

import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;

import java.time.Duration;
import java.util.Properties;

public final class KafkaSourceFactory {
    private KafkaSourceFactory() {}

    public static <T extends SpecificRecord> KafkaSource<T> avroSource(
            Class<T> type, String topic, String bootstrap, String registry, String groupId) {
        Properties props = new Properties();
        props.setProperty("specific.avro.reader", "true");
        return KafkaSource.<T>builder()
            .setBootstrapServers(bootstrap)
            .setTopics(topic)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(
                ConfluentRegistryAvroDeserializationSchema.forSpecific(type, registry))
            .setProperties(props)
            .build();
    }

    public static <T> WatermarkStrategy<T> watermarkStrategyTyped(
            SerializableToLong<T> tsExtractor) {
        return WatermarkStrategy.<T>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withIdleness(Duration.ofSeconds(30))
            .withTimestampAssigner((event, recordTs) -> tsExtractor.applyAsLong(event));
    }
}
