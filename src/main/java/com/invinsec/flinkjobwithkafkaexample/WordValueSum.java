package com.invinsec.flinkjobwithkafkaexample;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.graalvm.compiler.word.Word;

public class WordValueSum implements AggregateFunction<WordValue, WordValue, WordValue> {

    @Override
    public WordValue createAccumulator() {
        WordValue value = new WordValue();
        value.setValue(0);
        return value;
    }

    @Override
    public WordValue add(WordValue wordValue, WordValue accumulator) {
        accumulator.setValue(accumulator.getValue() + wordValue.getValue());
        return accumulator;
    }

    @Override
    public WordValue getResult(WordValue accumulator) {
        return accumulator;
    }

    @Override
    public WordValue merge(WordValue acc1, WordValue acc2) {
        WordValue acc = new WordValue();
        acc.setValue(acc1.getValue() + acc2.getValue());
        acc.setWord(acc1.getWord());
        return acc;
    }
}
