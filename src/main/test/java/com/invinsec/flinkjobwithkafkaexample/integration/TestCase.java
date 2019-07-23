package com.invinsec.flinkjobwithkafkaexample.integration;

import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.After;
import org.junit.Before;

public class TestCase {

    private static MiniClusterWithClientResource cluster;

    @Before
    public void clusterSetUp() throws Exception {
      MiniClusterResourceConfiguration config = new MiniClusterResourceConfiguration.Builder()
        .setNumberTaskManagers(2)
        .setNumberSlotsPerTaskManager(3)
        .build();
        cluster = new MiniClusterWithClientResource(config);
        cluster.before();
    }

    @After
    public void clusterClose() throws Exception {
      cluster.after();
    }

    protected MiniCluster getTestCluster() {
      return cluster.getMiniCluster();
    }
}
