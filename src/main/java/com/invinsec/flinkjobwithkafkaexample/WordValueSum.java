package com.invinsec.flinkjobwithkafkaexample;

import org.apache.flink.api.common.functions.ReduceFunction;

public class WordValueSum implements ReduceFunction<WordValue> {

  @Override
  public WordValue reduce(WordValue wv, WordValue otherWv) throws Exception {
    if (!wv.getWord().equals(otherWv.getWord())) {
      throw new Exception("Cannot sum 2 different word values");
    }
    return new WordValue(wv.getWord(), wv.getValue() + otherWv.getValue());
  }
}
