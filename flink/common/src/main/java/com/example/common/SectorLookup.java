package com.example.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.InputStream;
import java.util.Map;

public final class SectorLookup {
    private static final Map<String, String> MAP;
    static {
        try (InputStream in = SectorLookup.class.getResourceAsStream("/sectors.json")) {
            if (in == null) throw new IllegalStateException("sectors.json missing from classpath");
            MAP = new ObjectMapper().readValue(in, new TypeReference<Map<String, String>>() {});
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    private SectorLookup() {}
    public static String sectorFor(String symbol) {
        return MAP.getOrDefault(symbol, "UNKNOWN");
    }
    public static boolean isKnown(String symbol) {
        return MAP.containsKey(symbol);
    }
}
