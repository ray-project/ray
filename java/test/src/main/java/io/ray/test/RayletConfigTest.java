package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.runtime.config.RayConfig;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class RayletConfigTest extends BaseTest {

  private static final String RAY_CONFIG_KEY = "get_timeout_milliseconds";
  private static final String RAY_CONFIG_VALUE = "1234";

  @BeforeClass
  public void beforeClass() {
    RayConfig.forTestClass("ray.raylet.config." + RAY_CONFIG_KEY + ": " + RAY_CONFIG_VALUE);
  }

  public static class TestActor {

    public String getConfigValue() {
      return TestUtils.getRuntime()
          .getRayConfig()
          .rayletConfigParameters
          .get(RAY_CONFIG_KEY)
          .toString();
    }
  }

  @Test
  public void testRayletConfigPassThrough() {
    ActorHandle<TestActor> actor = Ray.actor(TestActor::new).remote();
    String configValue = actor.task(TestActor::getConfigValue).remote().get();
    Assert.assertEquals(configValue, RAY_CONFIG_VALUE);
  }
}
