package com.example.position;

import com.example.avro.Trade;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class LatenessRouter extends ProcessFunction<Trade, Trade> {
    public static final long LATENESS_THRESHOLD_MS = 60_000L;
    public static final OutputTag<Trade> DLQ_TAG = new OutputTag<>("dlq-late") {};

    private transient Counter dropped;

    @Override
    public void open(OpenContext ctx) {
        dropped = getRuntimeContext().getMetricGroup().counter("late_records_dropped_total");
    }

    @Override
    public void processElement(Trade t, Context ctx, Collector<Trade> out) {
        long wm = ctx.timerService().currentWatermark();
        long eventMs = t.getEventTs().toEpochMilli();
        if (wm > Long.MIN_VALUE && (wm - eventMs) > LATENESS_THRESHOLD_MS) {
            dropped.inc();
            ctx.output(DLQ_TAG, t);
        } else {
            out.collect(t);
        }
    }
}
