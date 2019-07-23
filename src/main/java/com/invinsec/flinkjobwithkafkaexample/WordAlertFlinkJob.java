package com.invinsec.flinkjobwithkafkaexample;


import lombok.Getter;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WordAlertFlinkJob {

  @Getter
  protected static long windowSize = 20;

  @Getter
  protected static long windowSlide = 5;

  public static void setup(
    SourceFunction<WordValue> controls,
    SourceFunction<WordValue> events,
    SinkFunction<String> sink
  ) {
    setup(env().addSource(controls), env().addSource(events), sink);
  }

  public static void setup(
    DataStreamSource<WordValue> controls,
    DataStreamSource<WordValue> events,
    SinkFunction<String> sink
  ) {

    env().setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

    events
      .name("events")
      .uid("events-source");

    controls
      .name("controls")
      .uid("controls-source");


    MapStateDescriptor<String, WordValue> stateDescriptor = new MapStateDescriptor<>(
      "alerts",
      Types.STRING,
      Types.POJO(WordValue.class)
    );

    BroadcastStream<WordValue> controlBroadcast = controls.broadcast(stateDescriptor);

    SingleOutputStreamOperator<String> output = events.keyBy("word")
      .window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlide)))
      .reduce(new WordValueSum())
      .keyBy("word")
      .connect(controlBroadcast)
      .process(new WordValueAlertFunction(stateDescriptor));

    if (sink != null) {
      output.addSink(sink);
    }
  }

  private static StreamExecutionEnvironment env() {
    return StreamExecutionEnvironment.getExecutionEnvironment();
  }

  /**
   * Alias for {@link StreamExecutionEnvironment#getExecutionEnvironment()#execute(String)}
   * @param jobName
   * @return
   * @throws Exception
   */
  public static JobExecutionResult execute(String jobName) throws Exception {
    return StreamExecutionEnvironment.getExecutionEnvironment().execute(jobName);
  }
}



