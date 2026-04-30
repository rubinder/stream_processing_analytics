package com.example.position;

import com.example.avro.Trade;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class LatenessRouter extends ProcessFunction<Trade, Trade> {
    public static final long LATENESS_THRESHOLD_MS = 60_000L;
    public static final OutputTag<Trade> DLQ_TAG = new OutputTag<>("dlq-late") {};

    @Override
    public void processElement(Trade t, Context ctx, Collector<Trade> out) {
        long wm = ctx.timerService().currentWatermark();
        long eventMs = t.getEventTs().toEpochMilli();
        if (wm > Long.MIN_VALUE && (wm - eventMs) > LATENESS_THRESHOLD_MS) {
            ctx.output(DLQ_TAG, t);
        } else {
            out.collect(t);
        }
    }
}
