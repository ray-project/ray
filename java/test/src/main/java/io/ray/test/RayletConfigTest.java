package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class RayletConfigTest extends BaseTest {

  private static final String RAY_CONFIG_KEY = "num_workers_per_process_java";
  private static final String RAY_CONFIG_VALUE = "2";

  @BeforeClass
  public void beforeClass() {
    System.setProperty("ray.raylet.config." + RAY_CONFIG_KEY, RAY_CONFIG_VALUE);
  }

  @AfterClass
  public void afterClass() {
    System.clearProperty("ray.raylet.config." + RAY_CONFIG_KEY);
  }

  public static class TestActor {

    public String getConfigValue() {
      return TestUtils.getRuntime().getRayConfig().rayletConfigParameters.get(RAY_CONFIG_KEY);
    }
  }

  @Test
  public void testRayletConfigPassThrough() {
    ActorHandle<TestActor> actor = Ray.actor(TestActor::new).remote();
    String configValue = actor.task(TestActor::getConfigValue).remote().get();
    Assert.assertEquals(configValue, RAY_CONFIG_VALUE);
  }
}
