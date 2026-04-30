package com.example.position;

import com.example.avro.Side;
import com.example.avro.Trade;
import com.example.avro.EnrichedTrade;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import static org.assertj.core.api.Assertions.assertThat;

class EnricherTest {

    @Test
    void enrichesBuyTradeWithSignedQtyAndNotional() {
        Trade t = Trade.newBuilder()
            .setTradeId("t1").setOrderId("o1").setClientId("C0001")
            .setSymbol("AAPL").setSide(Side.BUY).setQuantity(100L)
            .setPrice(new BigDecimal("150.0000")).setCurrency("USD").setVenue("NASDAQ")
            .setEventTs(Instant.ofEpochMilli(1000L)).build();

        EnrichedTrade out = new Enricher().map(t);

        assertThat(out.getSignedQty()).isEqualTo(100L);
        assertThat(out.getNotional()).isEqualByComparingTo(new BigDecimal("15000.00"));
        assertThat(out.getSector()).isEqualTo("Technology");
    }

    @Test
    void enrichesSellTradeWithNegativeSignedQty() {
        Trade t = Trade.newBuilder()
            .setTradeId("t1").setOrderId("o1").setClientId("C0001")
            .setSymbol("AAPL").setSide(Side.SELL).setQuantity(50L)
            .setPrice(new BigDecimal("200.0000")).setCurrency("USD").setVenue("NASDAQ")
            .setEventTs(Instant.ofEpochMilli(1000L)).build();

        EnrichedTrade out = new Enricher().map(t);

        assertThat(out.getSignedQty()).isEqualTo(-50L);
        assertThat(out.getNotional()).isEqualByComparingTo(new BigDecimal("10000.00"));
    }

    @Test
    void unknownSymbolGetsUnknownSector() {
        Trade t = Trade.newBuilder()
            .setTradeId("t1").setOrderId("o1").setClientId("C0001")
            .setSymbol("ZZZZ").setSide(Side.BUY).setQuantity(1L)
            .setPrice(new BigDecimal("1.0000")).setCurrency("USD").setVenue("NASDAQ")
            .setEventTs(Instant.ofEpochMilli(1L)).build();
        EnrichedTrade out = new Enricher().map(t);
        assertThat(out.getSector()).isEqualTo("UNKNOWN");
    }
}
