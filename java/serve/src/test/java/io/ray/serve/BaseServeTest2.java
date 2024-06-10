package io.ray.serve;

import io.ray.api.Ray;
import io.ray.serve.api.Serve;
import io.ray.serve.config.RayServeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public abstract class BaseServeTest2 {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseServeTest2.class);

  private String previousHttpPort;

  @BeforeMethod(alwaysRun = true)
  public void initConfig() {
    // The default port 8000 is occupied by other processes on the ci platform.
    previousHttpPort =
        System.setProperty(
            RayServeConfig.PROXY_HTTP_PORT, "8341"); // TODO(liuyang-my) Get an available port.
  }

  @AfterMethod(alwaysRun = true)
  public void shutdownServe() {
    try {
      Serve.shutdown();
    } catch (Exception e) {
      LOGGER.error("serve shutdown error", e);
    }
    try {
      Ray.shutdown();
      LOGGER.info("Base serve test shutdown ray. Is initialized:{}", Ray.isInitialized());
    } catch (Exception e) {
      LOGGER.error("ray shutdown error", e);
    }
    if (previousHttpPort == null) {
      System.clearProperty(RayServeConfig.PROXY_HTTP_PORT);
    } else {
      System.setProperty(RayServeConfig.PROXY_HTTP_PORT, previousHttpPort);
    }
  }
}
