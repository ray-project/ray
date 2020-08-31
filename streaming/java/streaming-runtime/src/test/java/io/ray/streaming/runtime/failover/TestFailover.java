package io.ray.streaming.runtime.failover;

import io.ray.streaming.api.context.StreamingContext;
import io.ray.streaming.api.function.impl.SourceFunction;
import io.ray.streaming.api.stream.DataStreamSource;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class TestFailover {

  private static final Logger LOG = LoggerFactory.getLogger(TestFailover.class);
  private static final String LOG_DIR = "/tmp/ray/session_latest";

  @Test(timeOut = 60000)
  public void simpleFailover() throws Exception {

    Map<String, String> conf = new HashMap<>();
    conf.put("streaming.state-backend.type", "LOCAL_FILE");

    // emit [1000, 1100) records
    int recordCount = new Random().nextInt(100) + 1000;

    StreamingContext streamingContext = StreamingContext.buildContext();
    DataStreamSource<Integer> source =
        DataStreamSource.fromSource(streamingContext, new DemoSource(recordCount));

    source.disableChain().sink(value -> {
      LOG.info("got value={}", value);
    });

    streamingContext.withConfig(conf);
    streamingContext.execute("simple_failover");

    // find source and kill it

    // find sink and kill it

    Thread.sleep(1000 * 300);

  }

  // this function depends on the log in JobWorker#init
  // be careful when changing the log in it
  private String findPidByWorkerName(String keyWords) {
    return null;
  }


  public static class DemoSource implements SourceFunction<Integer> {

    private static final Logger LOG = LoggerFactory.getLogger(DemoSource.class);

    private int value = 0;
    private int total = 0;

    public DemoSource(int total) {
      this.total = total;
    }

    @Override
    public void init(int totalParallel, int currentIndex) {
    }

    @Override
    public void fetch(
        SourceContext<Integer> ctx) throws Exception {
      Thread.sleep(10);
      if (value < total) {
        ctx.collect(value++);
      }
    }

    @Override
    public void close() {
    }

    @Override
    public void loadCheckpoint(Object checkpointObject) {
      value = (Integer) checkpointObject;
      LOG.info("load checkpoint, value={}, checkpointId={}", value, checkpointId);
    }

    @Override
    public Object doCheckpoint() {
      LOG.info("do checkpoint, value={}", value);
      return value;
    }
  }
}
