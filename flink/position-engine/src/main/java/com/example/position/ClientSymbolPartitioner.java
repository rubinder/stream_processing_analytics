package com.example.position;

import com.example.avro.Position;
import org.apache.flink.connector.kafka.sink.KafkaPartitioner;

public class ClientSymbolPartitioner implements KafkaPartitioner<Position> {
    @Override
    public int partition(Position record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        String compositeKey = record.getClientId() + "|" + record.getSymbol();
        int h = compositeKey.hashCode();
        return partitions[Math.floorMod(h, partitions.length)];
    }
}
