package com.invinsec.flinkjobwithkafkaexample.integration;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Arrays;
import java.util.List;

public class ParallelCollectionSource<T> extends RichSourceFunction<T> {

  private int index = 0;

  private List<T> list;

  private boolean canceled = false;

  public ParallelCollectionSource(T... array) {
    this.list = Arrays.asList(array);
  }

  public ParallelCollectionSource(List<T> list) {
    this.list = list;
  }

  @Override
  public void run(SourceContext<T> ctx) throws Exception {
    while (!canceled && index < list.size()) {
      T item = get();
      if (item == null) {
        break;
      }
      ctx.collect(item);
    }
  }

  synchronized protected T get() throws InterruptedException {
    wait(2000);
    if (index < list.size()) {
      return list.get(index++);
    }
    return null;
  }

  @Override
  public void cancel() {
    this.canceled = true;
  }
}
