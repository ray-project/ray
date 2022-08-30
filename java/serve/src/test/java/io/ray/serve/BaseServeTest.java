package io.ray.serve;

import io.ray.api.Ray;
import io.ray.serve.api.Serve;
import io.ray.serve.api.ServeControllerClient;
import io.ray.serve.config.RayServeConfig;
import io.ray.serve.poll.LongPollClientFactory;
import java.lang.reflect.Method;
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
  public void setUpBase(Method method) {
    Ray.init();
    Map<String, String> config = Maps.newHashMap();
    // The default port 8000 is occupied by other processes on the ci platform
    config.put(RayServeConfig.PROXY_HTTP_PORT, "8341");
    client = Serve.start(true, false, config);
  }

  @AfterMethod(alwaysRun = true)
  public void tearDownBase() {
    try {
      Serve.shutdown();
    } catch (Exception e) {
      LOGGER.error("serve shutdown error", e);
    }
    try {
      Ray.shutdown();
    } catch (Exception e) {
      LOGGER.error("ray shutdown error", e);
    }
    LongPollClientFactory.stop();
    LongPollClientFactory.clearAllCache();
    Serve.setInternalReplicaContext(null);
  }
}
