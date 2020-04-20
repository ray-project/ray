package io.ray.streaming.runtime.demo;

import io.ray.streaming.api.context.StreamingContext;
import io.ray.streaming.api.function.impl.FilterFunction;
import io.ray.streaming.api.function.impl.MapFunction;
import io.ray.streaming.api.stream.DataStreamSource;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

public class HybridStreamTest {

  public static class Mapper1 implements MapFunction<Object, Object> {

    @Override
    public Object map(Object value) {
      System.out.println("HybridStreamTest Mapper1 " + value);
      return value.toString();
    }
  }

  public static class Filter1 implements FilterFunction<Object> {

    @Override
    public boolean filter(Object value) throws Exception {
      System.out.println("HybridStreamTest Filter1 " + value);
      return !value.toString().contains("b");
    }
  }

  @Test
  public void testDataStream() throws InterruptedException {
    StreamingContext context = StreamingContext.buildContext();
    DataStreamSource<String> streamSource =
        DataStreamSource.fromCollection(context, Arrays.asList("a", "b", "c"));
    streamSource
        .map(x -> x + x)
        .asPython()
        .map("ray.streaming.tests.test_hybrid_stream", "map_func1")
        .filter("ray.streaming.tests.test_hybrid_stream", "filter_func1")
        .asJava()
        .sink(x -> System.out.println("HybridStreamTest: " + x));
    context.execute("HybridStreamTestJob");
    TimeUnit.SECONDS.sleep(3);
    context.stop();
  }

}
