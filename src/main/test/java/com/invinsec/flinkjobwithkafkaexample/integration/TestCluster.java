package com.invinsec.flinkjobwithkafkaexample.integration;

import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;

public class TestCluster {

  private MiniCluster cluster;

  public TestCluster() {
    MiniClusterConfiguration config = new MiniClusterConfiguration.Builder()
      .setNumSlotsPerTaskManager(2)
      .setNumTaskManagers(1)
      .build();

    cluster = new MiniCluster(config);
  }

}
