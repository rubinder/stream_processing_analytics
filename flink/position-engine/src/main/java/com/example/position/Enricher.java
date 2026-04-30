package com.example.position;

import com.example.avro.EnrichedTrade;
import com.example.avro.Side;
import com.example.avro.Trade;
import com.example.common.SectorLookup;
import org.apache.flink.api.common.functions.MapFunction;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class Enricher implements MapFunction<Trade, EnrichedTrade> {

    @Override
    public EnrichedTrade map(Trade t) {
        long signed = (t.getSide() == Side.BUY ? +1L : -1L) * t.getQuantity();
        BigDecimal notional = t.getPrice()
            .multiply(BigDecimal.valueOf(t.getQuantity()))
            .setScale(2, RoundingMode.HALF_UP);

        return EnrichedTrade.newBuilder()
            .setTradeId(t.getTradeId())
            .setOrderId(t.getOrderId())
            .setClientId(t.getClientId())
            .setSymbol(t.getSymbol())
            .setSide(t.getSide().name())
            .setQuantity(t.getQuantity())
            .setSignedQty(signed)
            .setPrice(t.getPrice())
            .setNotional(notional)
            .setCurrency(t.getCurrency())
            .setVenue(t.getVenue())
            .setSector(SectorLookup.sectorFor(t.getSymbol()))
            .setEventTs(t.getEventTs())
            .build();
    }
}
