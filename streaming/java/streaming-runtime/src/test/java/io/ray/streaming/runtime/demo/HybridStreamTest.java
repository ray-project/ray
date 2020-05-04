package io.ray.streaming.runtime.demo;

import io.ray.api.Ray;
import io.ray.streaming.api.context.StreamingContext;
import io.ray.streaming.api.function.impl.FilterFunction;
import io.ray.streaming.api.function.impl.MapFunction;
import io.ray.streaming.api.stream.DataStreamSource;
import io.ray.streaming.runtime.BaseUnitTest;
import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class HybridStreamTest extends BaseUnitTest implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(HybridStreamTest.class);

  public static class Mapper1 implements MapFunction<Object, Object> {

    @Override
    public Object map(Object value) {
      LOG.info("HybridStreamTest Mapper1 {}", value);
      return value.toString();
    }
  }

  public static class Filter1 implements FilterFunction<Object> {

    @Override
    public boolean filter(Object value) throws Exception {
      LOG.info("HybridStreamTest Filter1 {}", value);
      return !value.toString().contains("b");
    }
  }

  @Test
  public void testHybridDataStream() throws InterruptedException {
    Ray.shutdown();
    StreamingContext context = StreamingContext.buildContext();
    DataStreamSource<String> streamSource =
        DataStreamSource.fromCollection(context, Arrays.asList("a", "b", "c"));
    streamSource
        .map(x -> x + x)
        .asPythonStream()
        .map("ray.streaming.tests.test_hybrid_stream", "map_func1")
        .filter("ray.streaming.tests.test_hybrid_stream", "filter_func1")
        .asJavaStream()
        .sink(x -> System.out.println("HybridStreamTest: " + x));
    context.execute("HybridStreamTestJob");
    TimeUnit.SECONDS.sleep(3);
    context.stop();
    LOG.info("HybridStreamTest succeed");
  }

}
