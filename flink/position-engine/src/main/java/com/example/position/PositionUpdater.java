package com.example.position;

import com.example.avro.EnrichedTrade;
import com.example.avro.Position;
import com.example.avro.Side;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;

public class PositionUpdater extends KeyedProcessFunction<String, EnrichedTrade, Position> {

    public static final OutputTag<EnrichedTrade> ENRICHED_TAG = new OutputTag<>("enriched") {};

    public static class State {
        public long netQuantity;
        public BigDecimal weightedCostBasis = BigDecimal.ZERO;
        public BigDecimal lastPrice = BigDecimal.ZERO;
        public long lastEventTsMs = Long.MIN_VALUE;
        public long lastUpdateMs;
        public String sector = "UNKNOWN";
    }

    private transient ValueState<State> stateHolder;

    @Override
    public void open(OpenContext ctx) {
        stateHolder = getRuntimeContext().getState(
            new ValueStateDescriptor<>("position", State.class));
    }

    @Override
    public void processElement(EnrichedTrade t, Context ctx, Collector<Position> out) throws Exception {
        State s = stateHolder.value();
        if (s == null) s = new State();

        BigDecimal price = t.getPrice();
        long eventMs = t.getEventTs().toEpochMilli();

        long previousNet = s.netQuantity;
        s.netQuantity += t.getSignedQty();

        if (t.getSide() == Side.BUY) {
            BigDecimal addQty = BigDecimal.valueOf(t.getQuantity());
            BigDecimal prevQty = BigDecimal.valueOf(Math.max(previousNet, 0L));
            BigDecimal numerator = s.weightedCostBasis.multiply(prevQty).add(price.multiply(addQty));
            BigDecimal denominator = prevQty.add(addQty);
            s.weightedCostBasis = denominator.signum() == 0
                ? BigDecimal.ZERO
                : numerator.divide(denominator, 4, RoundingMode.HALF_UP);
        }

        if (eventMs > s.lastEventTsMs) {
            s.lastPrice = price;
            s.lastEventTsMs = eventMs;
        }
        s.lastUpdateMs = Math.max(s.lastUpdateMs, eventMs);
        s.sector = t.getSector();

        BigDecimal notional = s.lastPrice
            .multiply(BigDecimal.valueOf(s.netQuantity))
            .setScale(2, RoundingMode.HALF_UP);

        ctx.output(ENRICHED_TAG, t);

        out.collect(Position.newBuilder()
            .setClientId(t.getClientId())
            .setSymbol(t.getSymbol())
            .setNetQuantity(s.netQuantity)
            .setAvgPrice(s.weightedCostBasis.setScale(4, RoundingMode.HALF_UP))
            .setNotional(notional)
            .setSector(s.sector)
            .setLastUpdate(Instant.ofEpochMilli(s.lastUpdateMs))
            .build());

        stateHolder.update(s);
    }
}
