package org.ray.streaming.runtime.demo;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.ray.streaming.api.context.StreamingContext;
import org.ray.streaming.api.function.impl.FilterFunction;
import org.ray.streaming.api.function.impl.MapFunction;
import org.ray.streaming.api.stream.DataStreamSource;
import org.testng.annotations.Test;

public class CrossLangStreamTest {

  public static class Mapper1 implements MapFunction<Object, Object> {

    @Override
    public Object map(Object value) {
      return value.toString();
    }
  }

  public static class Filter1 implements FilterFunction<Object> {

    @Override
    public boolean filter(Object value) throws Exception {
      return value.toString().contains("a");
    }
  }

  @Test
  public void testDataStream() throws InterruptedException {
    StreamingContext context = StreamingContext.buildContext();
    DataStreamSource<String> streamSource =
        DataStreamSource.fromCollection(context, Arrays.asList("a", "b", "c"));
    streamSource.map(x -> x + x)
        .setParallelism(2)
        .asPython()
        .map("ray.streaming.tests.test_cross_lang_stream", "map_func1")
        .filter("ray.streaming.tests.test_cross_lang_stream", "filter_func1")
        .asJava()
        .sink(x -> System.out.println(x));
    context.execute("CrossLangStreamTestJob");
    TimeUnit.SECONDS.sleep(5);
  }

}
