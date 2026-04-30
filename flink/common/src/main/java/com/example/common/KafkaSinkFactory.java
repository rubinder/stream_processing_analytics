package com.example.common;

import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;

import java.util.Properties;
import java.util.function.Function;

public final class KafkaSinkFactory {
    private KafkaSinkFactory() {}

    public static <T extends SpecificRecord> KafkaSink<T> avroSink(
            Class<T> type, String topic, String bootstrap, String registry,
            Function<T, byte[]> keyExtractor, String txnIdPrefix) {
        Properties txnProps = new Properties();
        txnProps.setProperty("transaction.timeout.ms", "300000");
        return KafkaSink.<T>builder()
            .setBootstrapServers(bootstrap)
            .setKafkaProducerConfig(txnProps)
            .setTransactionalIdPrefix(txnIdPrefix)
            .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
            .setRecordSerializer(KafkaRecordSerializationSchema.<T>builder()
                .setTopic(topic)
                .setKeySerializationSchema(keyExtractor::apply)
                .setValueSerializationSchema(
                    ConfluentRegistryAvroSerializationSchema.forSpecific(type, topic + "-value", registry))
                .build())
            .build();
    }
}
