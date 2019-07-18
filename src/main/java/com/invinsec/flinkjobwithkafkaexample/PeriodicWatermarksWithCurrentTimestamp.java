package com.invinsec.flinkjobwithkafkaexample;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class PeriodicWatermarksWithCurrentTimestamp implements AssignerWithPeriodicWatermarks {

  private long milliseconds = Long.MAX_VALUE;


  public PeriodicWatermarksWithCurrentTimestamp() {

  }

  public PeriodicWatermarksWithCurrentTimestamp(long miliseconds) {
    this.milliseconds = miliseconds;
  }

  @Override
  public Watermark getCurrentWatermark() {
    return new Watermark(milliseconds);
  }

  @Override
  public long extractTimestamp(Object element, long previousElementTimestamp) {
    return System.currentTimeMillis();
  }
}
