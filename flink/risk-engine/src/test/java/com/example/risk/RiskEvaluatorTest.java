package com.example.risk;

import com.example.avro.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithKeyedOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RiskEvaluatorTest {

    private static ClientLimit limit(String c, String gross, String single, int tpm) {
        return ClientLimit.newBuilder()
            .setClientId(c)
            .setMaxGrossNotional(new BigDecimal(gross).setScale(2))
            .setMaxSingleOrderNotional(new BigDecimal(single).setScale(2))
            .setMaxTradesPerMinute(tpm)
            .setUpdatedTs(Instant.EPOCH).build();
    }

    private static EnrichedTrade trade(String c, String s, long qty, String price, long ts) {
        BigDecimal p = new BigDecimal(price);
        BigDecimal n = p.multiply(BigDecimal.valueOf(qty)).setScale(2);
        return EnrichedTrade.newBuilder()
            .setTradeId("t-" + ts).setOrderId("o-" + ts).setClientId(c).setSymbol(s).setSide(Side.BUY)
            .setQuantity(qty).setSignedQty(qty).setPrice(p).setNotional(n)
            .setCurrency("USD").setVenue("X").setSector("X")
            .setEventTs(Instant.ofEpochMilli(ts)).build();
    }

    private static Position pos(String c, String s, long net, String last, String notional, long ts) {
        return Position.newBuilder().setClientId(c).setSymbol(s).setNetQuantity(net)
            .setAvgPrice(new BigDecimal(last).setScale(4))
            .setNotional(new BigDecimal(notional).setScale(2))
            .setSector("X").setLastUpdate(Instant.ofEpochMilli(ts)).build();
    }

    private static KeyedTwoInputStreamOperatorTestHarness<String, RiskInput, ClientLimit, Breach> harness() throws Exception {
        var op = new CoBroadcastWithKeyedOperator<>(
            new RiskEvaluator(),
            List.of(RiskEvaluator.LIMITS_DESC));
        var h = new KeyedTwoInputStreamOperatorTestHarness<>(
            op,
            (org.apache.flink.api.java.functions.KeySelector<RiskInput, String>) RiskInput::clientId,
            (org.apache.flink.api.java.functions.KeySelector<ClientLimit, String>) ClientLimit::getClientId,
            Types.STRING);
        h.open();
        return h;
    }

    @Test
    void singleOrderNotionalBreachFires() throws Exception {
        try (var h = harness()) {
            h.processElement2(new StreamRecord<>(limit("C1", "1000000", "10000", 60), 0L));
            h.processElement1(new StreamRecord<>(new RiskInput.OfTrade(trade("C1", "AAPL", 100, "200.0000", 1L)), 1L));
            assertThat(h.extractOutputValues()).hasSize(1);
            assertThat(h.extractOutputValues().get(0).getBreachType()).isEqualTo(BreachType.SINGLE_ORDER_NOTIONAL);
        }
    }

    @Test
    void grossNotionalBreachFires() throws Exception {
        try (var h = harness()) {
            h.processElement2(new StreamRecord<>(limit("C1", "5000", "999999999", 60), 0L));
            h.processElement1(new StreamRecord<>(new RiskInput.OfPosition(pos("C1", "AAPL", 100, "100.0000", "10000.00", 1L)), 1L));
            h.processElement1(new StreamRecord<>(new RiskInput.OfTrade(trade("C1", "MSFT", 1, "1.0000", 2L)), 2L));
            assertThat(h.extractOutputValues()).hasSize(1);
            assertThat(h.extractOutputValues().get(0).getBreachType()).isEqualTo(BreachType.GROSS_NOTIONAL);
        }
    }

    @Test
    void velocityBreachFires() throws Exception {
        try (var h = harness()) {
            h.processElement2(new StreamRecord<>(limit("C1", "9999999", "9999999", 3), 0L));
            for (int i = 0; i < 4; i++) {
                long ts = 1_000L + i * 1_000L;
                h.processElement1(new StreamRecord<>(new RiskInput.OfTrade(trade("C1", "AAPL", 1, "1.0000", ts)), ts));
            }
            long velocityCount = h.extractOutputValues().stream()
                .filter(b -> b.getBreachType() == BreachType.TRADE_VELOCITY).count();
            assertThat(velocityCount).isGreaterThanOrEqualTo(1);
        }
    }

    @Test
    void noLimitConfiguredYieldsNoBreaches() throws Exception {
        try (var h = harness()) {
            h.processElement1(new StreamRecord<>(new RiskInput.OfTrade(trade("C1", "AAPL", 1_000_000, "1000.0000", 1L)), 1L));
            assertThat(h.extractOutputValues()).isEmpty();
        }
    }

    @Test
    void stalePositionEventIsIgnored() throws Exception {
        try (var h = harness()) {
            h.processElement2(new StreamRecord<>(limit("C1", "1000", "999999999", 60), 0L));
            h.processElement1(new StreamRecord<>(new RiskInput.OfPosition(pos("C1", "AAPL", 1, "10.0000", "10.00", 100L)), 100L));
            h.processElement1(new StreamRecord<>(new RiskInput.OfPosition(pos("C1", "AAPL", 1, "10.0000", "10.00",  50L)),  50L));
            h.processElement1(new StreamRecord<>(new RiskInput.OfTrade(trade("C1", "MSFT", 1, "1.0000", 200L)), 200L));
            long grossCount = h.extractOutputValues().stream()
                .filter(b -> b.getBreachType() == BreachType.GROSS_NOTIONAL).count();
            assertThat(grossCount).isEqualTo(0);
        }
    }
}
