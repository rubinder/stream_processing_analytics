package com.example.position;

import com.example.avro.EnrichedTrade;
import com.example.avro.Position;
import com.example.avro.Trade;
import com.example.common.KafkaSinkFactory;
import com.example.common.KafkaSourceFactory;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class PositionEngineJob {

    public static void main(String[] args) throws Exception {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "kafka:9092");
        String registry  = System.getenv().getOrDefault("SCHEMA_REGISTRY", "http://schema-registry:8081");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(2);
        env.enableCheckpointing(30_000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(Duration.ofMinutes(2).toMillis());

        var tradeSource = KafkaSourceFactory.avroSource(
            Trade.class, "trades", bootstrap, registry, "position-engine");

        WatermarkStrategy<Trade> ws = KafkaSourceFactory.watermarkStrategyTyped(
            t -> t.getEventTs().toEpochMilli());

        DataStream<Trade> tradesRaw = env.fromSource(tradeSource, ws, "trades-source");

        // Lateness routing: > 60s late → DLQ side output
        SingleOutputStreamOperator<Trade> ontime = tradesRaw
            .process(new LatenessRouter()).name("lateness-router");

        // (DLQ sink is not wired in v1 — see plan's known limitations.)

        // Enrich, key by (client_id, symbol), update position state.
        SingleOutputStreamOperator<Position> positions = ontime
            .map(new Enricher()).name("enricher")
            .keyBy(e -> e.getClientId() + "|" + e.getSymbol())
            .process(new PositionUpdater()).name("position-updater");

        DataStream<EnrichedTrade> enriched = positions.getSideOutput(PositionUpdater.ENRICHED_TAG);

        KafkaSink<EnrichedTrade> enrichedSink = KafkaSinkFactory.avroSink(
            EnrichedTrade.class, "enriched-trades", bootstrap, registry,
            e -> e.getClientId().getBytes(),
            "position-engine-enriched");

        KafkaSink<Position> positionSink = KafkaSinkFactory.avroSink(
            Position.class, "positions", bootstrap, registry,
            p -> (p.getClientId() + "|" + p.getSymbol()).getBytes(),
            "position-engine-position");

        enriched.sinkTo(enrichedSink).name("enriched-sink");
        positions.sinkTo(positionSink).name("position-sink");

        env.execute("position-engine");
    }
}
