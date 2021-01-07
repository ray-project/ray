package io.ray.streaming.runtime.demo;

import io.ray.api.Ray;
import io.ray.streaming.api.context.StreamingContext;
import io.ray.streaming.api.function.impl.SinkFunction;
import io.ray.streaming.api.stream.DataStreamSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UnionStreamTest {

  private static final Logger LOG = LoggerFactory.getLogger(UnionStreamTest.class);

  @Test(timeOut = 60000)
  public void testUnionStream() throws Exception {
    Ray.shutdown();
    String sinkFileName = "/tmp/testUnionStream.txt";
    Files.deleteIfExists(Paths.get(sinkFileName));

    StreamingContext context = StreamingContext.buildContext();
    DataStreamSource<Integer> streamSource1 =
        DataStreamSource.fromCollection(context, Arrays.asList(1, 1));
    DataStreamSource<Integer> streamSource2 =
        DataStreamSource.fromCollection(context, Arrays.asList(1, 1));
    DataStreamSource<Integer> streamSource3 =
        DataStreamSource.fromCollection(context, Arrays.asList(1, 1));
    streamSource1
        .union(streamSource2, streamSource3)
        .sink(
            (SinkFunction<Integer>)
                value -> {
                  LOG.info("UnionStreamTest, sink: {}", value);
                  try {
                    if (!Files.exists(Paths.get(sinkFileName))) {
                      Files.createFile(Paths.get(sinkFileName));
                    }
                    Files.write(
                        Paths.get(sinkFileName),
                        value.toString().getBytes(),
                        StandardOpenOption.APPEND);
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                });
    context.execute("UnionStreamTest");
    int sleptTime = 0;
    TimeUnit.SECONDS.sleep(3);
    while (true) {
      if (Files.exists(Paths.get(sinkFileName))) {
        TimeUnit.SECONDS.sleep(3);
        String text = String.join(", ", Files.readAllLines(Paths.get(sinkFileName)));
        Assert.assertEquals(text, StringUtils.repeat("1", 6));
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
