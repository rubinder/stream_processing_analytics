package com.example.position;

import com.example.avro.EnrichedTrade;
import com.example.avro.Position;
import com.example.avro.Side;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import static org.assertj.core.api.Assertions.assertThat;

class PositionUpdaterTest {

    private static EnrichedTrade et(String tid, String sym, Side side, long qty, String price, long ts) {
        long signed = (side == Side.BUY ? 1L : -1L) * qty;
        BigDecimal p = new BigDecimal(price);
        BigDecimal n = p.multiply(BigDecimal.valueOf(qty)).setScale(2);
        return EnrichedTrade.newBuilder()
            .setTradeId(tid).setOrderId("o").setClientId("C0001").setSymbol(sym)
            .setSide(side).setQuantity(qty).setSignedQty(signed)
            .setPrice(p).setNotional(n)
            .setCurrency("USD").setVenue("X").setSector("Technology")
            .setEventTs(Instant.ofEpochMilli(ts)).build();
    }

    private static KeyedOneInputStreamOperatorTestHarness<String, EnrichedTrade, Position> harness() throws Exception {
        var op = new KeyedProcessOperator<>(new PositionUpdater());
        var h = new KeyedOneInputStreamOperatorTestHarness<>(
            op, e -> e.getClientId() + "|" + e.getSymbol(), Types.STRING);
        h.open();
        return h;
    }

    @Test
    void buyThenSellAccumulatesNetQuantity() throws Exception {
        try (var h = harness()) {
            h.processElement(new StreamRecord<>(et("t1", "AAPL", Side.BUY, 100, "150.0000", 1_000L), 1_000L));
            h.processElement(new StreamRecord<>(et("t2", "AAPL", Side.SELL, 30, "160.0000", 2_000L), 2_000L));
            var out = h.extractOutputValues();
            assertThat(out).hasSize(2);
            assertThat(out.get(1).getNetQuantity()).isEqualTo(70L);
            assertThat(out.get(1).getNotional()).isEqualByComparingTo(new BigDecimal("11200.00"));
        }
    }

    @Test
    void lateTradeDoesNotRegressLastPrice() throws Exception {
        try (var h = harness()) {
            h.processElement(new StreamRecord<>(et("t1", "AAPL", Side.BUY, 10, "200.0000", 5_000L), 5_000L));
            h.processElement(new StreamRecord<>(et("t2", "AAPL", Side.BUY,  5, "100.0000", 2_000L), 2_000L));
            var out = h.extractOutputValues();
            assertThat(out.get(1).getNetQuantity()).isEqualTo(15L);
            assertThat(out.get(1).getNotional()).isEqualByComparingTo(new BigDecimal("3000.00"));
        }
    }

    @Test
    void lastUpdateIsMonotonic() throws Exception {
        try (var h = harness()) {
            h.processElement(new StreamRecord<>(et("t1", "AAPL", Side.BUY, 10, "10.0000", 5_000L), 5_000L));
            h.processElement(new StreamRecord<>(et("t2", "AAPL", Side.BUY, 10, "10.0000", 1_000L), 1_000L));
            var out = h.extractOutputValues();
            assertThat(out.get(0).getLastUpdate().toEpochMilli()).isEqualTo(5_000L);
            assertThat(out.get(1).getLastUpdate().toEpochMilli()).isEqualTo(5_000L);
        }
    }
}
