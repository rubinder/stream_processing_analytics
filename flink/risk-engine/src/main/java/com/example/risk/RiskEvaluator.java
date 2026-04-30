package com.example.risk;

import com.example.avro.*;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.UUID;

public class RiskEvaluator extends KeyedBroadcastProcessFunction<String, RiskInput, ClientLimit, Breach> {

    public static final MapStateDescriptor<String, ClientLimit> LIMITS_DESC =
        new MapStateDescriptor<>("limits", Types.STRING, Types.GENERIC(ClientLimit.class));

    public static class SymbolNotional {
        public BigDecimal notional;
        public long lastUpdateMs;
        public SymbolNotional() {}
        public SymbolNotional(BigDecimal n, long ts) { this.notional = n; this.lastUpdateMs = ts; }
    }

    private transient MapState<String, SymbolNotional> symbolNotionals;
    private transient ListState<Long> recentTimestamps;
    private transient org.apache.flink.metrics.Counter breachesGross;
    private transient org.apache.flink.metrics.Counter breachesSingle;
    private transient org.apache.flink.metrics.Counter breachesVelocity;

    @Override
    public void open(OpenContext ctx) {
        symbolNotionals = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("symbol-notionals", Types.STRING, Types.GENERIC(SymbolNotional.class)));
        recentTimestamps = getRuntimeContext().getListState(
            new ListStateDescriptor<>("recent-ts", Types.LONG));
        var mg = getRuntimeContext().getMetricGroup();
        breachesGross    = mg.counter("breaches_emitted_gross_notional");
        breachesSingle   = mg.counter("breaches_emitted_single_order");
        breachesVelocity = mg.counter("breaches_emitted_velocity");
    }

    @Override
    public void processBroadcastElement(ClientLimit limit, Context ctx, Collector<Breach> out) throws Exception {
        ctx.getBroadcastState(LIMITS_DESC).put(limit.getClientId(), limit);
    }

    @Override
    public void processElement(RiskInput input, ReadOnlyContext ctx, Collector<Breach> out) throws Exception {
        if (input instanceof RiskInput.OfPosition op) {
            handlePosition(op.position());
        } else if (input instanceof RiskInput.OfTrade ot) {
            evaluate(ot.trade(), ctx, out);
        }
    }

    private void handlePosition(Position p) throws Exception {
        long ts = p.getLastUpdate().toEpochMilli();
        SymbolNotional existing = symbolNotionals.get(p.getSymbol());
        if (existing != null && ts <= existing.lastUpdateMs) return;
        if (p.getNetQuantity() == 0L) {
            symbolNotionals.remove(p.getSymbol());
        } else {
            symbolNotionals.put(p.getSymbol(), new SymbolNotional(p.getNotional().abs(), ts));
        }
    }

    private void evaluate(EnrichedTrade t, ReadOnlyContext ctx, Collector<Breach> out) throws Exception {
        ClientLimit limit = ctx.getBroadcastState(LIMITS_DESC).get(t.getClientId());
        if (limit == null) return;

        long eventMs = t.getEventTs().toEpochMilli();
        BigDecimal tradeNotional = t.getNotional();

        // Rule 1: SINGLE_ORDER_NOTIONAL
        BigDecimal singleLimit = limit.getMaxSingleOrderNotional();
        if (tradeNotional.compareTo(singleLimit) > 0) {
            emit(out, t.getClientId(), BreachType.SINGLE_ORDER_NOTIONAL, singleLimit, tradeNotional,
                t.getTradeId(), t.getOrderId(), eventMs,
                "order notional " + tradeNotional + " exceeds " + singleLimit);
            breachesSingle.inc();
        }

        // Rule 2: GROSS_NOTIONAL — sum across all symbols
        BigDecimal gross = BigDecimal.ZERO;
        for (SymbolNotional sn : symbolNotionals.values()) {
            gross = gross.add(sn.notional);
        }
        BigDecimal grossLimit = limit.getMaxGrossNotional();
        if (gross.compareTo(grossLimit) > 0) {
            emit(out, t.getClientId(), BreachType.GROSS_NOTIONAL, grossLimit, gross,
                t.getTradeId(), t.getOrderId(), eventMs,
                "gross notional " + gross + " exceeds " + grossLimit);
            breachesGross.inc();
        }

        // Rule 3: TRADE_VELOCITY — sliding 60s window
        long cutoff = eventMs - 60_000L;
        ArrayList<Long> kept = new ArrayList<>();
        for (Long tsMs : recentTimestamps.get()) if (tsMs >= cutoff) kept.add(tsMs);
        kept.add(eventMs);
        recentTimestamps.update(kept);
        if (kept.size() > limit.getMaxTradesPerMinute()) {
            emit(out, t.getClientId(), BreachType.TRADE_VELOCITY,
                BigDecimal.valueOf(limit.getMaxTradesPerMinute()),
                BigDecimal.valueOf(kept.size()),
                t.getTradeId(), t.getOrderId(), eventMs,
                kept.size() + " trades in 60s window");
            breachesVelocity.inc();
        }
    }

    private void emit(Collector<Breach> out, String cid, BreachType type, BigDecimal limit, BigDecimal actual,
                      String tradeId, String orderId, long tsMs, String details) {
        out.collect(Breach.newBuilder()
            .setBreachId(UUID.randomUUID().toString())
            .setClientId(cid).setBreachType(type)
            .setLimitValue(limit.setScale(2, RoundingMode.HALF_UP))
            .setActualValue(actual.setScale(2, RoundingMode.HALF_UP))
            .setTradeId(tradeId)
            .setOrderId(type == BreachType.SINGLE_ORDER_NOTIONAL ? orderId : null)
            .setDetectedTs(Instant.ofEpochMilli(tsMs))
            .setDetails(details).build());
    }
}
