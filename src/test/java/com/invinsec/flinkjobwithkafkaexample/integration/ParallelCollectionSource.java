package com.invinsec.flinkjobwithkafkaexample.integration;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class ParallelCollectionSource<T> implements SourceFunction<T> {

  private int index = 0;

  private List<T> list;

  private boolean canceled = false;

  private long interval = 1000;

  public ParallelCollectionSource(T... array) {
    this.list = Arrays.asList(array);
  }

  public ParallelCollectionSource(long milliSecondsInterval, T... array) {
    this.interval = milliSecondsInterval < 0 ? 0 : milliSecondsInterval;
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
    Thread.sleep(interval);
    if (index < list.size()) {
      T item = list.get(index++);
      System.out.println(Instant.now().toString() + ": receiving: " + item.toString());
      return item;
    }
    return null;
  }

  @Override
  public void cancel() {
    this.canceled = true;
  }
}
