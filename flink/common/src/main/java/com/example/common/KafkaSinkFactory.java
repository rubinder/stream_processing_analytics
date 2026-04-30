package com.example.common;

import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;

import java.util.Properties;

public final class KafkaSinkFactory {
    private KafkaSinkFactory() {}

    public static <T extends SpecificRecord> KafkaSink<T> avroSink(
            Class<T> type, String topic, String bootstrap, String registry,
            SerializableKeyExtractor<T> keyExtractor, String txnIdPrefix) {
        Properties txnProps = new Properties();
        txnProps.setProperty("transaction.timeout.ms", "300000");
        SerializationSchema<T> keySchema = new KeySerializationSchema<>(keyExtractor);
        return KafkaSink.<T>builder()
            .setBootstrapServers(bootstrap)
            .setKafkaProducerConfig(txnProps)
            .setTransactionalIdPrefix(txnIdPrefix)
            .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
            .setRecordSerializer(KafkaRecordSerializationSchema.<T>builder()
                .setTopic(topic)
                .setKeySerializationSchema(keySchema)
                .setValueSerializationSchema(
                    ConfluentRegistryAvroSerializationSchema.forSpecific(type, topic + "-value", registry))
                .build())
            .build();
    }

    private static final class KeySerializationSchema<T> implements SerializationSchema<T> {
        private final SerializableKeyExtractor<T> extractor;
        KeySerializationSchema(SerializableKeyExtractor<T> extractor) { this.extractor = extractor; }
        @Override public byte[] serialize(T element) { return extractor.apply(element); }
    }
}
