package com.example.risk;

import com.example.avro.EnrichedTrade;
import com.example.avro.Position;

/** Tagged union for the keyed input of RiskEvaluator. */
public sealed interface RiskInput {
    String clientId();

    record OfTrade(EnrichedTrade trade) implements RiskInput {
        public String clientId() { return trade.getClientId(); }
    }
    record OfPosition(Position position) implements RiskInput {
        public String clientId() { return position.getClientId(); }
    }
}
