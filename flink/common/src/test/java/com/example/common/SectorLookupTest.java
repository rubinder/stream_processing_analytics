package com.example.common;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class SectorLookupTest {
    @Test
    void resolvesKnownSymbol() {
        assertThat(SectorLookup.sectorFor("AAPL")).isEqualTo("Technology");
    }
    @Test
    void unknownSymbolReturnsUnknown() {
        assertThat(SectorLookup.sectorFor("ZZZZ")).isEqualTo("UNKNOWN");
    }
    @Test
    void isKnownReportsTruthForMappedSymbol() {
        assertThat(SectorLookup.isKnown("AAPL")).isTrue();
        assertThat(SectorLookup.isKnown("ZZZZ")).isFalse();
    }
}
