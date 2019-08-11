package com.invinsec.flinkjobwithkafkaexample.integration;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

// create a testing sink
public class CollectSink<T> implements SinkFunction<T> {

  // MUST BE STATIC, otherwise sink will be empty. This is because Flink serialization process
  public static final List<Object> values = new ArrayList<>();

  public List getValues() {
    return values;
  }

  public List<T> getValues(Class<T> cls) {
    return values.stream().map(cls::cast).collect(Collectors.toList());
  }

  public List<T> getSortedValues(Class<T> cls) {
    return values.stream().map(cls::cast).sorted().collect(Collectors.toList());
  }

  @Override
  synchronized public void invoke(T value) throws Exception {
    values.add(value);
//    System.out.println(value);
  }

  @Override
  synchronized public void invoke(T value, Context context) throws Exception {
    System.out.println(value);
    values.add(value);
  }
}
