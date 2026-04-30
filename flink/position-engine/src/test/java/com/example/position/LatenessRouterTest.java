package com.example.position;

import com.example.avro.Side;
import com.example.avro.Trade;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import static org.assertj.core.api.Assertions.assertThat;

class LatenessRouterTest {

    private static Trade trade(long eventTsMs) {
        return Trade.newBuilder()
            .setTradeId("t").setOrderId("o").setClientId("C0001")
            .setSymbol("AAPL").setSide(Side.BUY).setQuantity(1L)
            .setPrice(new BigDecimal("1.0000")).setCurrency("USD").setVenue("X")
            .setEventTs(Instant.ofEpochMilli(eventTsMs)).build();
    }

    @Test
    void onTimeRoutedToMain() throws Exception {
        var op = new ProcessOperator<>(new LatenessRouter());
        try (var h = new OneInputStreamOperatorTestHarness<>(op)) {
            h.open();
            h.processWatermark(100_000L);                                  // wm=100s
            h.processElement(new StreamRecord<>(trade(99_000L), 99_000L)); // 1s late, ok
            assertThat(h.extractOutputValues()).hasSize(1);
            assertThat(h.getSideOutput(LatenessRouter.DLQ_TAG)).isNullOrEmpty();
        }
    }

    @Test
    void veryLateRoutedToDlq() throws Exception {
        var op = new ProcessOperator<>(new LatenessRouter());
        try (var h = new OneInputStreamOperatorTestHarness<>(op)) {
            h.open();
            h.processWatermark(200_000L);                                  // wm=200s
            h.processElement(new StreamRecord<>(trade(100_000L), 100_000L)); // 100s late
            assertThat(h.extractOutputValues()).isEmpty();
            assertThat(h.getSideOutput(LatenessRouter.DLQ_TAG)).hasSize(1);
        }
    }
}
