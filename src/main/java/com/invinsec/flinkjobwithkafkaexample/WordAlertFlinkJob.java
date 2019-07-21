package com.invinsec.flinkjobwithkafkaexample;


import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;


public class WordAlertFlinkJob {

  final static private String EVENT_QUEUE_NAME = "events";
  final static private String CONTROL_QUEUE_NAME = "controls";

  private static String kafkaServer = "localhost:9092";

  public static void main(String[] args) throws Exception {

//    final int port;
//    try {
//      final ParameterTool params = ParameterTool.fromArgs(args);
//      port = params.getInt("port");
//    } catch (Exception e) {
//      System.err.println("No port specified. Please run 'WordAlertFlinkJob --port <port>'");
//      return;
//    }

    // get the execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // get input data by connecting to the kafka
    DataStreamSource<WordValue> eventsSource = env.addSource(getEventSource());
    eventsSource.name("events").uid("events-source");
    eventsSource.keyBy("word");

    DataStreamSource<WordValue> controlSource = env.addSource(getControlSource());
    controlSource.name("controls").uid("controls-source");

    MapStateDescriptor<String, WordValue> stateDescriptor = new MapStateDescriptor<>(
      "alerts",
      Types.STRING,
      Types.POJO(WordValue.class)
    );

    BroadcastStream<WordValue> controlBroadcast = controlSource.broadcast(stateDescriptor);

    eventsSource.keyBy("word")
      .window(SlidingProcessingTimeWindows.of(Time.minutes(1), Time.seconds(10)))
      .aggregate(new WordValueSum())
      .connect(controlBroadcast)
      .process(new WordValueAlertFunction(stateDescriptor));

    ;

//    DataStream<String> text = env.socketTextStream("localhost", port, "\n");
      /*
      this that this:23 these:2
       */
    env.execute("Socket Window WordCount");
  }

  private static SourceFunction<WordValue> getEventSource() {
    return getKafkaStreamConsumer(EVENT_QUEUE_NAME, kafkaServer, new WordValueDeserializationSchema());
  }

  private static SourceFunction<WordValue> getControlSource() {
    return getKafkaStreamConsumer(CONTROL_QUEUE_NAME, kafkaServer, new WordValueDeserializationSchema()
    );
  }

  private static FlinkKafkaConsumer<WordValue> getKafkaStreamConsumer(
    String topic,
    String server,
    DeserializationSchema<WordValue> deserializer
  ) {

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", server);

    FlinkKafkaConsumer<WordValue> kafka = new FlinkKafkaConsumer<>(
      topic,
      deserializer,
      properties
    );

    return kafka;
  }
}



