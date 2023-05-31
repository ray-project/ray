package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.exception.RayException;
import io.ray.runtime.util.SystemUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class AdvancedFailureTest extends BaseTest {

  private static final Logger LOG = LoggerFactory.getLogger(AdvancedFailureTest.class);

  @BeforeClass
  public void setUp() {
    System.setProperty("ray.task.return_task_exception", "false");
    System.setProperty("ray.job.jvm-options.0", "-Dray.task.return_task_exception=false");
    System.setProperty("ray.job.enable-l1-fault-tolerance", "true");
    System.setProperty("ray.job.num-java-workers-per-process", "3");
  }

  public static class Counter {
    private Integer count = 0;

    public Integer increment() {
      this.count += 1;
      return this.count;
    }
  }

  public static Integer function(ActorHandle<Counter> counter) {
    ObjectRef<Integer> count = counter.task(Counter::increment).remote();
    if (count.get() == 1) {
      System.exit(0);
      return 0;
    } else {
      return 1;
    }
  }

  public static Integer functionReturnException(ActorHandle<Counter> counter) {
    ObjectRef<Integer> count = counter.task(Counter::increment).remote();
    if (count.get() == 1) {
      throw new RayException("worker exits.");
    } else {
      return 1;
    }
  }

  @Test(groups = "cluster")
  public void testTaskFailure() throws InterruptedException {
    ActorHandle<Counter> counter = Ray.actor(Counter::new).remote();
    ObjectRef<Integer> returnObject =
        Ray.task(AdvancedFailureTest::function, counter).setMaxRetries(1).remote();
    Assert.assertTrue(1 == returnObject.get());

    ActorHandle<Counter> counter2 = Ray.actor(Counter::new).remote();
    ObjectRef<Integer> returnObject2 = Ray.task(AdvancedFailureTest::function, counter2).remote();
    try {
      returnObject2.get();
    } catch (Exception e) {
      LOG.info("Worker has failed.");
      Assert.assertTrue(e instanceof RayException);
    }

    ActorHandle<Counter> counter3 = Ray.actor(Counter::new).remote();
    ObjectRef<Integer> returnObject3 =
        Ray.task(AdvancedFailureTest::functionReturnException, counter3).remote();
    Assert.assertTrue(1 == returnObject.get());
    try {
      returnObject3.get();
    } catch (Exception e) {
      LOG.info("Worker has failed.");
      Assert.assertTrue(e instanceof RayException);
    }
  }

}
