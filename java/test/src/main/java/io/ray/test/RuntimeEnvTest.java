package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.runtimeenv.RuntimeEnv;
import io.ray.runtime.util.SystemUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "cluster")
public class RuntimeEnvTest extends BaseTest {

  @BeforeClass
  public void setUp() {
    /// This is used to test that actors with runtime envs will not reuse worker process.
    System.setProperty("ray.job.num-java-workers-per-process", "2");
  }

  private static class A {

    public String getEnv(String key) {
      return System.getenv(key);
    }

    public int getPid() {
      return SystemUtil.pid();
    }
  }

  public void testEnvironmentVariable() {
    int pid1 = 0;
    int pid2 = 0;
    {
      RuntimeEnv runtimeEnv =
          new RuntimeEnv.Builder()
              .addEnvVar("KEY1", "A")
              .addEnvVar("KEY2", "B")
              .addEnvVar("KEY1", "C")
              .build();

      ActorHandle<A> actor1 = Ray.actor(A::new).setRuntimeEnv(runtimeEnv).remote();
      String val = actor1.task(A::getEnv, "KEY1").remote().get();
      Assert.assertEquals(val, "C");
      val = actor1.task(A::getEnv, "KEY2").remote().get();
      Assert.assertEquals(val, "B");

      pid1 = actor1.task(A::getPid).remote().get();
    }

    {
      /// Because we didn't set them for actor2 , all should be null.
      ActorHandle<A> actor2 = Ray.actor(A::new).remote();
      String val = actor2.task(A::getEnv, "KEY1").remote().get();
      Assert.assertNull(val);
      val = actor2.task(A::getEnv, "KEY2").remote().get();
      Assert.assertNull(val);
      pid2 = actor2.task(A::getPid).remote().get();
    }

    // actor1 and actor2 shouldn't be in one process because they have
    // different runtime env.
    Assert.assertNotEquals(pid1, pid2);
  }
}
