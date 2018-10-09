package org.ray.api.test;

import java.util.Arrays;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.config.RunMode;
import org.ray.runtime.config.WorkerMode;

public class RayConfigTest {

  @Before
  public void setUp() throws Exception {
    System.clearProperty("ray.home");
  }

  @Test
  public void testRayConfigWithSystemProperty() {
    System.setProperty("ray.home", "/path/to/ray1");
    RayConfig rayConfig = RayConfig.create("config-test.conf");

    Assert.assertEquals("/path/to/ray1", rayConfig.rayHome);
    Assert.assertEquals(WorkerMode.DRIVER, rayConfig.workerMode);
    Assert.assertEquals(RunMode.CLUSTER, rayConfig.runMode);

    System.setProperty("ray.home", "");
    rayConfig = RayConfig.create("config-test.conf");
    Assert.assertEquals(System.getProperty("user.dir"), rayConfig.rayHome);
  }

  @Test
  public void testDefaultRayConfig() {
    RayConfig rayConfig = RayConfig.create(null);
    Assert.assertEquals("./build/src/common/thirdparty/redis/src/redis-server",
        rayConfig.redisServerExecutablePath);
  }

  @Test
  public void testRayFallback() {
    RayConfig rayConfig = RayConfig.create("config-test.conf");
    Assert.assertEquals("/path/to/ray", rayConfig.rayHome);
    Assert.assertEquals("/path/to/ray/build/src/common/thirdparty/redis/src/redis-server",
        rayConfig.redisServerExecutablePath);
    Assert.assertEquals(Arrays.asList(
        "/path/to/ray/java/conf",
        "/path/to/ray/java/runtime/target/classes",
        "/path/to/ray/java/test/target/classes",
        "/path/to/ray/java/api/target/classes",
        "/path/to/ray/java/tutorial/target/classes",
        "/path/to/ray/java/assembly/lib/*"),
        rayConfig.classpath);
  }
}
