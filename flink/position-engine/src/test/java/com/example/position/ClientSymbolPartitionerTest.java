package com.example.position;

import com.example.avro.Position;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import static org.assertj.core.api.Assertions.assertThat;

class ClientSymbolPartitionerTest {

    private static Position p(String c, String s) {
        return Position.newBuilder()
            .setClientId(c).setSymbol(s).setNetQuantity(0L)
            .setAvgPrice(BigDecimal.ZERO).setNotional(BigDecimal.ZERO)
            .setSector("X").setLastUpdate(Instant.EPOCH).build();
    }

    @Test
    void samePairAlwaysSamePartition() {
        var part = new ClientSymbolPartitioner();
        int p1 = part.partition(p("C1", "AAPL"), null, null, "positions", new int[]{0, 1, 2, 3});
        int p2 = part.partition(p("C1", "AAPL"), null, null, "positions", new int[]{0, 1, 2, 3});
        assertThat(p1).isEqualTo(p2);
        assertThat(p1).isBetween(0, 3);
    }

    @Test
    void differentPairsCanBeDifferentPartitions() {
        var part = new ClientSymbolPartitioner();
        int[] parts = new int[]{0, 1, 2, 3};
        int seenMask = 0;
        for (int i = 0; i < 50; i++) {
            seenMask |= (1 << part.partition(p("C" + i, "S" + i), null, null, "positions", parts));
        }
        assertThat(Integer.bitCount(seenMask)).isGreaterThan(1);
    }
}
