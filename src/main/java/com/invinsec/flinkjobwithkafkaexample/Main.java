package com.invinsec.flinkjobwithkafkaexample;


import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;
import java.util.regex.Pattern;

import static org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS;


public class Main {

  final static private String EVENT_QUEUE_NAME = "(.+-)?events";
  final static private String CONTROL_QUEUE_NAME = "(.+-)?controls";

  private static String kafkaServer = "kafka:9092";

  public static void main(String[] args) throws Exception {
    WordAlertFlinkJob.setup(
      getControlSource(),
      getEventSource(),
      getKafkaStreamProducer("output", kafkaServer)
    );
    WordAlertFlinkJob.execute("Socket Window WordCount");
  }

  private static SourceFunction<WordValue> getEventSource() {
    return getKafkaStreamConsumer(Pattern.compile(EVENT_QUEUE_NAME), kafkaServer, new WordValueDeserializationSchema());
  }

  private static SourceFunction<WordValue> getControlSource() {
    return getKafkaStreamConsumer(Pattern.compile(CONTROL_QUEUE_NAME), kafkaServer, new WordValueDeserializationSchema()
    );
  }

  private static FlinkKafkaProducer<String> getKafkaStreamProducer(String topic, String server) {

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", server);
    properties.setProperty(KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "5000");

    FlinkKafkaProducer<String> kafka = new FlinkKafkaProducer<>(
      topic,
      (SerializationSchema<String>) element -> element.getBytes(),
      properties
    );

    return kafka;
  }

  private static FlinkKafkaConsumer<WordValue> getKafkaStreamConsumer(
    String topic,
    String server,
    DeserializationSchema<WordValue> deserializer
  ) {

    Properties properties = new Properties();
    properties.setProperty(KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "5000");
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);

    FlinkKafkaConsumer<WordValue> kafka = new FlinkKafkaConsumer<>(
      topic,
      deserializer,
      properties
    );

    return kafka;
  }

  private static FlinkKafkaConsumer<WordValue> getKafkaStreamConsumer(
    Pattern topicPattern,
    String server,
    DeserializationSchema<WordValue> deserializer
  ) {

    Properties properties = new Properties();
    properties.setProperty(KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "5000");
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);

    FlinkKafkaConsumer<WordValue> kafka = new FlinkKafkaConsumer<>(
      topicPattern,
      deserializer,
      properties
    );

    return kafka;
  }
}



