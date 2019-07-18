package com.invinsec.flinkjobwithkafkaexample;

import lombok.Data;

@Data
public class WordValue {

  private String word;

  private Integer value = 1;

}
