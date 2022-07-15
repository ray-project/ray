package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class LoggingPerfTest extends BaseTest {

  private static final Logger LOG = LoggerFactory.getLogger(LoggingPerfTest.class);

  private static class LoggingPerfActor {

    public Long measure() {
      final int count = 2000000;
      final long startMs = System.currentTimeMillis();
      for (int i = 0; i < count; ++i) {
        LOG.info("hello world");
      }
      final long endMs = System.currentTimeMillis();
      return endMs - startMs;
    }
  }

  @Test
  public void testLoggingPerf() {
    ActorHandle<LoggingPerfActor> actor = Ray.actor(LoggingPerfActor::new).remote();
    ObjectRef<Long> took = actor.task(LoggingPerfActor::measure).remote();
    LOG.info("It took {} milliseconds.", took.get());
  }
}
