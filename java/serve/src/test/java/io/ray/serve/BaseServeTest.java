package io.ray.serve;

import io.ray.api.Ray;
import io.ray.serve.api.Serve;
import io.ray.serve.api.ServeControllerClient;
import io.ray.serve.poll.LongPollClientFactory;
import java.lang.reflect.Method;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public abstract class BaseServeTest {
  protected static ServeControllerClient client = null;

  @BeforeMethod(alwaysRun = true)
  public void setUpBase(Method method) {
    Assert.assertFalse(Ray.isInitialized());
    Ray.init();
    client = Serve.start(true, false, null, null);
  }

  @AfterMethod(alwaysRun = true)
  public void tearDownBase() {
    Serve.shutdown();
    client.shutdown();
    Ray.shutdown();
    LongPollClientFactory.clearAllCache();
    Serve.setInternalReplicaContext(null);
    Serve.setGlobalClient(null);
  }
}
