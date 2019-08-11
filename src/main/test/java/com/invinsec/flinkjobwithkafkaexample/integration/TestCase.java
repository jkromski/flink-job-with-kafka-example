package com.invinsec.flinkjobwithkafkaexample.integration;

import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.After;
import org.junit.Before;

import static java.time.Instant.now;

public class TestCase {

    protected static MiniClusterWithClientResource cluster;

    @Before
    public void clearSinks() {
      CollectSink.values.clear();
    }

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
      System.out.println("after");
      cluster.after();
    }

    protected MiniCluster getTestCluster() {
      return cluster.getMiniCluster();
    }

    protected void waitForWindowToStart(long windowSlideInSeconds) throws InterruptedException {
      long secondsToNextWindow = windowSlideInSeconds - now().getEpochSecond() % windowSlideInSeconds;
      System.out.println(now().toString() + ": next window in " + secondsToNextWindow);
      Thread.sleep(secondsToNextWindow * 1000);
    }
}
