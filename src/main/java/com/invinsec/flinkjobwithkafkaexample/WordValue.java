package com.invinsec.flinkjobwithkafkaexample;

import lombok.*;
import org.apache.flink.optimizer.plantranslate.JsonMapper;

import java.io.Serializable;

@Data
@NoArgsConstructor
@RequiredArgsConstructor
@AllArgsConstructor
public class WordValue implements Serializable {

  @NonNull
  private String word;

  private Integer value = 1;

  public String toString() {
    return String.format("{'%s': %s}", word, value);
  }

}
