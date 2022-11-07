package io.ray.serve;

import io.ray.api.Ray;
import io.ray.serve.api.Serve;
import io.ray.serve.api.ServeControllerClient;
import io.ray.serve.common.Constants;
import io.ray.serve.config.RayServeConfig;
import io.ray.serve.poll.LongPollClientFactory;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.collections.Maps;

public abstract class BaseServeTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseServeTest.class);
  protected static ServeControllerClient client = null;

  @BeforeMethod(alwaysRun = true)
  public static void startServe() {
    Map<String, String> config = Maps.newHashMap();
    // The default port 8000 is occupied by other processes on the ci platform.
    config.put(RayServeConfig.PROXY_HTTP_PORT, "8341"); // TODO(liuyang-my) Get an available port.
    client = Serve.start(true, false, config);
  }

  @AfterMethod(alwaysRun = true)
  public static void shutdownServe() {
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
  }

  private static boolean previousInited;

  private static String previousNamespace;

  public static void initRay() {
    previousInited = Ray.isInitialized();
    LOGGER.info("Base serve test. Previous inited:{}", previousInited);
    previousNamespace = System.getProperty("ray.job.namespace");
    LOGGER.info("Base serve test. Previous namespace:{}", previousNamespace);

    System.setProperty("ray.job.namespace", Constants.SERVE_NAMESPACE);
    Ray.init();
  }

  public static void shutdownRay() {
    if (!previousInited) {
      Ray.shutdown();
      LOGGER.info("Base serve test shutdown ray. Is initialized:{}", Ray.isInitialized());
    }
    if (previousNamespace == null) {
      System.clearProperty("ray.job.namespace");
    } else {
      System.setProperty("ray.job.namespace", previousNamespace);
    }
  }

  public static void clearContext() {
    Serve.clearContext();
  }

  public static void stopLongPoll() {
    LongPollClientFactory.stop();
  }

  public static void clearAndShutdownRay() {
    stopLongPoll();
    shutdownRay();
    clearContext();
  }
}
