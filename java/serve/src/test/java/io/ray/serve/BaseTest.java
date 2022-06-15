package io.ray.serve;

import io.ray.api.Ray;
import io.ray.serve.api.Serve;
import io.ray.serve.common.Constants;

public class BaseTest {

  private boolean originInited = false;

  protected void init() {
    originInited = Ray.isInitialized();
    if (!originInited) {
      System.setProperty("ray.job.namespace", Constants.SERVE_NAMESPACE);
      Ray.init();
    }
  }

  protected void shutdown() {
    if (!originInited) {
      Ray.shutdown();
    }
    clear();
  }

  protected void clear() {
    Serve.setInternalReplicaContext(null);
    Serve.setGlobalClient(null);
  }
}
