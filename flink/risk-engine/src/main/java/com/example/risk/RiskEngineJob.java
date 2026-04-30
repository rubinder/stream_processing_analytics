package com.example.risk;

import com.example.avro.*;
import com.example.common.KafkaSinkFactory;
import com.example.common.KafkaSourceFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RiskEngineJob {

    public static void main(String[] args) throws Exception {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "kafka:9092");
        String registry  = System.getenv().getOrDefault("SCHEMA_REGISTRY", "http://schema-registry:8081");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(30_000L, CheckpointingMode.EXACTLY_ONCE);

        var tradeSrc = KafkaSourceFactory.avroSource(EnrichedTrade.class, "enriched-trades", bootstrap, registry, "risk-engine-trades");
        var posSrc   = KafkaSourceFactory.avroSource(Position.class,      "positions",       bootstrap, registry, "risk-engine-positions");
        var limSrc   = KafkaSourceFactory.avroSource(ClientLimit.class,   "client-limits",   bootstrap, registry, "risk-engine-limits");

        WatermarkStrategy<EnrichedTrade> wsTrade =
            KafkaSourceFactory.watermarkStrategyTyped(t -> t.getEventTs().toEpochMilli());
        WatermarkStrategy<Position> wsPos =
            KafkaSourceFactory.watermarkStrategyTyped(p -> p.getLastUpdate().toEpochMilli());

        DataStream<EnrichedTrade> trades = env.fromSource(tradeSrc, wsTrade, "enriched-trades-source");
        DataStream<Position>      poss   = env.fromSource(posSrc,   wsPos,   "positions-source");
        DataStream<ClientLimit>   lims   = env.fromSource(limSrc,   WatermarkStrategy.noWatermarks(), "client-limits-source");

        DataStream<RiskInput> tagged = trades
            .map(t -> (RiskInput) new RiskInput.OfTrade(t)).returns(RiskInput.class)
            .union(poss.map(p -> (RiskInput) new RiskInput.OfPosition(p)).returns(RiskInput.class));

        BroadcastStream<ClientLimit> broadcast = lims.broadcast(RiskEvaluator.LIMITS_DESC);

        SingleOutputStreamOperator<Breach> breaches = tagged
            .keyBy(RiskInput::clientId)
            .connect(broadcast)
            .process(new RiskEvaluator())
            .name("risk-evaluator");

        KafkaSink<Breach> breachSink = KafkaSinkFactory.avroSink(
            Breach.class, "breaches", bootstrap, registry,
            b -> b.getClientId().getBytes(),
            "risk-engine-breach");

        breaches.sinkTo(breachSink).name("breach-sink");

        env.execute("risk-engine");
    }
}
