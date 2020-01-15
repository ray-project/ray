package org.ray.api.test;

import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.TestUtils;
import org.ray.api.annotation.RayRemote;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SingleProcessModeTest extends BaseTest {

  @RayRemote
  static class MyActor {

    public MyActor() {
    }

    public long getThreadId() {
      return Thread.currentThread().getId();
    }
  }

  @Test
  public void testActorTasksInOneThread() {
    TestUtils.skipTestUnderClusterMode();

    RayActor<MyActor> actor = Ray.createActor(MyActor::new);
    Long threadId = Ray.call(MyActor::getThreadId, actor).get();
    for (int i = 0; i < 100; ++i) {
      Assert.assertEquals(threadId, Ray.call(MyActor::getThreadId, actor).get());
    }
  }
}
