package io.ray.streaming.runtime.demo;

import io.ray.api.Ray;
import io.ray.streaming.api.context.StreamingContext;
import io.ray.streaming.api.function.impl.FilterFunction;
import io.ray.streaming.api.function.impl.MapFunction;
import io.ray.streaming.api.function.impl.SinkFunction;
import io.ray.streaming.api.stream.DataStreamSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class HybridStreamTest {
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

  @Test(timeOut = 60000)
  public void testHybridDataStream() throws Exception {
    Ray.shutdown();
    String sinkFileName = "/tmp/testHybridDataStream.txt";
    Files.deleteIfExists(Paths.get(sinkFileName));

    StreamingContext context = StreamingContext.buildContext();
    DataStreamSource<String> streamSource =
        DataStreamSource.fromCollection(context, Arrays.asList("a", "b", "c"));
    streamSource
        .map(x -> x + x)
        .asPythonStream()
        .map("ray.streaming.tests.test_hybrid_stream", "map_func1")
        .filter("ray.streaming.tests.test_hybrid_stream", "filter_func1")
        .asJavaStream()
        .sink((SinkFunction<Object>) value -> {
          LOG.info("HybridStreamTest: {}", value);
          try {
            if (!Files.exists(Paths.get(sinkFileName))) {
              Files.createFile(Paths.get(sinkFileName));
            }
            Files.write(Paths.get(sinkFileName), value.toString().getBytes(),
                StandardOpenOption.APPEND);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
    context.execute("HybridStreamTestJob");
    int sleptTime = 0;
    TimeUnit.SECONDS.sleep(3);
    while (true) {
      if (Files.exists(Paths.get(sinkFileName))) {
        TimeUnit.SECONDS.sleep(3);
        String text = String.join(", ", Files.readAllLines(Paths.get(sinkFileName)));
        Assert.assertTrue(text.contains("a"));
        Assert.assertFalse(text.contains("b"));
        Assert.assertTrue(text.contains("c"));
        LOG.info("Execution succeed");
        break;
      }
      sleptTime += 1;
      if (sleptTime >= 60) {
        throw new RuntimeException("Execution not finished");
      }
      LOG.info("Wait finish...");
      TimeUnit.SECONDS.sleep(1);
    }
    context.stop();
    LOG.info("HybridStreamTest succeed");
  }

}
