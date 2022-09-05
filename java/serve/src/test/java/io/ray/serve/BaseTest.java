package io.ray.serve;

import io.ray.api.Ray;
import io.ray.serve.api.Serve;
import io.ray.serve.common.Constants;
import io.ray.serve.poll.LongPollClientFactory;

public class BaseTest {

  private boolean previousInited = false;

  private String previousNamespace = null;

  protected void init() {
    previousInited = Ray.isInitialized();
    previousNamespace = System.getProperty("ray.job.namespace");
    System.setProperty("ray.job.namespace", Constants.SERVE_NAMESPACE);
    Ray.init();
  }

  protected void shutdown() {
    LongPollClientFactory.stop();
    LongPollClientFactory.clearAllCache();
    Serve.setInternalReplicaContext(null);
    if (!previousInited) {
      Ray.shutdown();
    }
    if (previousNamespace == null) {
      System.clearProperty("ray.job.namespace");
    } else {
      System.setProperty("ray.job.namespace", previousNamespace);
    }
    clear();
  }

  protected void clear() {
    Serve.setInternalReplicaContext(null);
    Serve.setGlobalClient(null);
  }
}
