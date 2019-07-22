package com.invinsec.flinkjobwithkafkaexample;

import lombok.*;
import java.io.Serializable;

@Data
@NoArgsConstructor
@RequiredArgsConstructor
@AllArgsConstructor
public class WordValue implements Serializable {

  @NonNull
  private String word;

  private Integer value = 1;

}
