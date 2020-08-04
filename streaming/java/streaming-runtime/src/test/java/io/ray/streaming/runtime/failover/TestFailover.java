package io.ray.streaming.runtime.failover;

import io.ray.streaming.api.context.StreamingContext;
import io.ray.streaming.api.function.impl.SourceFunction;
import io.ray.streaming.api.stream.DataStreamSource;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class TestFailover {

  private static final Logger LOG = LoggerFactory.getLogger(TestFailover.class);

  @Test
  public void simpleFailover() throws Exception {

    Map<String, String> conf = new HashMap<>();
    conf.put("streaming.state-backend.type", "LOCAL_FILE");

    StreamingContext streamingContext = StreamingContext.buildContext();
    DataStreamSource<Integer> source =
      DataStreamSource.fromSource(streamingContext, new DemoSource());

    source.disableChain().sink(value -> {
      LOG.info("got value={}", value);
    });

    streamingContext.withConfig(conf);
    streamingContext.execute("simple_failover");

    Thread.sleep(1000 * 300);

  }


  public static class DemoSource implements SourceFunction<Integer> {

    private static final Logger LOG = LoggerFactory.getLogger(DemoSource.class);

    private int value = 0;

    @Override
    public void init(int totalParallel, int currentIndex) {
    }

    @Override
    public void fetch(
      SourceContext<Integer> ctx, long checkpointId) throws Exception {
      ctx.collect(value++);
    }

    @Override
    public void close() {
    }

    @Override
    public void loadCheckpoint(Object checkpointObject, long checkpointId) {
      value = (Integer) checkpointObject;
      LOG.info("load checkpoint, value={}, checkpointId={}", value, checkpointId);
    }

    @Override
    public Object doCheckpoint(long checkpointId) {
      LOG.info("do checkpoint, value={}, checkpointId={}", value, checkpointId);
      return value;
    }
  }
}
