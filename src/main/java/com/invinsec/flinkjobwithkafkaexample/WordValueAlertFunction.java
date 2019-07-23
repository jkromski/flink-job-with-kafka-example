package com.invinsec.flinkjobwithkafkaexample;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;

public class WordValueAlertFunction extends KeyedBroadcastProcessFunction<String, WordValue, WordValue, String> {

    private final MapStateDescriptor<String, WordValue> stateDescriptor;

    public WordValueAlertFunction(MapStateDescriptor<String, WordValue> stateDescriptor) {
        this.stateDescriptor = stateDescriptor;
    }

    @Override
    public void processElement(
            WordValue value,
            KeyedBroadcastProcessFunction.ReadOnlyContext ctx,
            Collector out
    ) throws Exception {
        WordValue alertOnValue = ctx.getBroadcastState(stateDescriptor).get(value.getWord());

        System.out.println(Instant.now().toString() + ": checking: " + value.toString());

        if (alertOnValue != null && alertOnValue.getValue() <= value.getValue()) {
            out.collect(
                    String.format(
                            "alert for '%s' %d >= %d",
                            value.getWord(),
                            value.getValue(),
                            alertOnValue.getValue()
                    )
            );
        }
    }

    @Override
    public void processBroadcastElement(
            WordValue value,
            KeyedBroadcastProcessFunction.Context ctx,
            Collector out
    ) throws Exception {
        ctx.getBroadcastState(stateDescriptor).put(value.getWord(), value);
    }
}

