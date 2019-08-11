package com.invinsec.flinkjobwithkafkaexample.integration;

import com.google.common.collect.Lists;
import com.invinsec.flinkjobwithkafkaexample.WordAlertFlinkJob;
import com.invinsec.flinkjobwithkafkaexample.WordValue;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import java.time.Instant;

import static com.invinsec.flinkjobwithkafkaexample.WordAlertFlinkJob.getWindowSize;
import static com.invinsec.flinkjobwithkafkaexample.WordAlertFlinkJob.getWindowSlide;
import static java.time.Instant.now;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class SimpleJobTest extends TestCase {

  @Test
  public void simple() throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);

    WordValue alertWhen = new WordValue("a", 1);

    DataStreamSource<WordValue> controls = env.fromElements(alertWhen);

    DataStreamSource<WordValue> events = env.addSource(
      // to process a window we need to reach end of it,
      // that is why 6 elements with element released each second
      new ParallelCollectionSource<WordValue>(
        1000,
        new WordValue("b"),
        new WordValue("a"),
        new WordValue("b"),
        new WordValue("c"),
        new WordValue("c"),
        new WordValue("c")
      ),
      PojoTypeInfo.of(WordValue.class)
    );

    CollectSink sink = new CollectSink();

    WordAlertFlinkJob.setup(controls, events, sink);
    JobGraph jobGraph = env.getStreamGraph().getJobGraph();

    // wait for next window start to make test consistent
    waitForWindowToStart(getWindowSlide());


    getTestCluster().submitJob(jobGraph).get();
    JobResult result = getTestCluster().requestJobResult(jobGraph.getJobID()).join();

    assertTrue(result.isSuccess());

    assertEquals(
      1,
      sink.getValues().size()
    );
    assertEquals("alert for 'a' 1 >= 1", sink.getValues().get(0));
  }

  @Test
  public void changeControlValueWhenProcessing() throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);

    WordValue alertWhen = new WordValue("a", 1);

    DataStreamSource<WordValue> controls = env.addSource(
      new ParallelCollectionSource<WordValue>(1000,
        alertWhen,
        new WordValue("a", 3)
      ),
      PojoTypeInfo.of(WordValue.class)
    );

    DataStreamSource<WordValue> events = env.addSource(
      // to process a window we need to reach end of it,
      // that is why 6 elements with element released each second
      new ParallelCollectionSource<WordValue>(
        1000,
        new WordValue("b"),
        new WordValue("b"),
        new WordValue("a"),
        new WordValue("a"),
        new WordValue("c"),
        new WordValue("c")
      ),
      PojoTypeInfo.of(WordValue.class)
    );

    CollectSink sink = new CollectSink();

    WordAlertFlinkJob.setup(controls, events, sink);
    JobGraph jobGraph = env.getStreamGraph().getJobGraph();
    // wait for next window start to make test consistent
    long secondsToNextWindow = getWindowSlide() - now().getEpochSecond() % getWindowSlide();
    System.out.println(now().toString() + ": next window in " + secondsToNextWindow);

    Thread.sleep(secondsToNextWindow * 1000);

    getTestCluster().submitJob(jobGraph).get();
    JobResult result = getTestCluster().requestJobResult(jobGraph.getJobID()).join();

    assertTrue(result.isSuccess());

    assertEquals(
      0,
      sink.getValues().size()
    );
  }

  @Test
  public void checkCluster() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // configure your test environment
    env.setParallelism(2);

    CollectSink<Long> sink = new CollectSink<>();

    // create a stream of custom elements and apply transformations
    env.fromElements(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L)
      .map(x -> x * 2)
      .addSink(sink);

    JobGraph jobGraph = env.getStreamGraph().getJobGraph();

    getTestCluster().submitJob(jobGraph).get();

    JobResult result = getTestCluster().requestJobResult(jobGraph.getJobID()).get();


    assertEquals(
      Lists.newArrayList(2L, 4L, 6L, 8L, 10L, 12L, 14L, 16L, 18L, 20L),
      sink.getSortedValues(Long.class)
    );
  }
}
