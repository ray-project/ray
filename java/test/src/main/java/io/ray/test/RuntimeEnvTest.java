package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.runtimeenv.RuntimeEnv;
import io.ray.api.runtimeenv.RuntimeEnvBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class RuntimeEnvTest extends BaseTest {

  private static class A {

    public String getEnv(String key) {
      return System.getenv(key);
    }

  }

  public void testEnvironmentVariable() {
    RuntimeEnv runtimeEnv = new RuntimeEnvBuilder()
      .addEnvVar("KEY1", "A")
      .addEnvVar("KEY2", "B")
      .addEnvVar("KEY1", "C")
      .build();

    ActorHandle<A> actor = Ray.actor(A::new).setRuntimeEnv(runtimeEnv).remote();
    String val = actor.task(A::getEnv, "KEY1").remote().get();
    Assert.assertEquals(val, "C");
    val = actor.task(A::getEnv, "KEY2").remote().get();
    Assert.assertEquals(val, "B");
  }

}
