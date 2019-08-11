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


public class Main {

  final static private String EVENT_QUEUE_NAME = "events";
  final static private String CONTROL_QUEUE_NAME = "controls";

  private static String kafkaServer = "kafka:9092";

  public static void main(String[] args) throws Exception {
    WordAlertFlinkJob.setup(getControlSource(), getEventSource(), null);
    WordAlertFlinkJob.execute("Socket Window WordCount");
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



