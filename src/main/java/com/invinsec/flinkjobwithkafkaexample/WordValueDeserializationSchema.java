package com.invinsec.flinkjobwithkafkaexample;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

@Slf4j
public class WordValueDeserializationSchema implements DeserializationSchema<WordValue> {

  @Override
  public WordValue deserialize(byte[] byteMessage) {

    try {

      String message = new String(byteMessage);

      String[] splitted = message.split(":");
      
      if (splitted.length < 1 || splitted[0].isEmpty()) {
        throw new IllegalArgumentException("unable to deserialize");
      }

      WordValue wordValue = new WordValue();

      wordValue.setWord(splitted[0]);

      if (splitted.length > 1) {
        wordValue.setValue(Integer.parseInt(splitted[1]));
      }

      return wordValue;

    } catch (Exception e) {
      log.error("de-serialization error", e);
    }
    return null;
  }

  @Override
  public boolean isEndOfStream(WordValue nextElement) {
    return false;
  }

  @Override
  public TypeInformation<WordValue> getProducedType() {
    return TypeExtractor.getForClass(WordValue.class);
  }
}
