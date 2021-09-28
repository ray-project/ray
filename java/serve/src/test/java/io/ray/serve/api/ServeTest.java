package io.ray.serve.api;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.serve.Constants;
import io.ray.serve.DummyServeController;
import io.ray.serve.RayServeException;
import io.ray.serve.ReplicaContext;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ServeTest {

  @Test
  public void replicaContextTest() {

    // Get current context.
    ReplicaContext preContext = null;
    try {
      preContext = Serve.getReplicaContext();
    } catch (RayServeException e) {
    }

    // Test context setting and getting.
    String backendTag = "backendTag";
    String replicaTag = "replicaTag";
    String controllerName = "controllerName";
    Object servableObject = new Object();
    Serve.setInternalReplicaContext(backendTag, replicaTag, controllerName, servableObject);

    ReplicaContext replicaContext = Serve.getReplicaContext();
    Assert.assertNotNull(replicaContext, "no replica context");
    Assert.assertEquals(replicaContext.getBackendTag(), backendTag);
    Assert.assertEquals(replicaContext.getReplicaTag(), replicaTag);
    Assert.assertEquals(replicaContext.getInternalControllerName(), controllerName);

    // Recover context.
    Serve.setInternalReplicaContext(preContext);
  }

  @SuppressWarnings("unused")
  @Test
  public void getGlobalClientTest() {
    boolean inited = Ray.isInitialized();
    Ray.init();
    try {
      Client client = null;
      try {
        client = Serve.getGlobalClient();
        Assert.assertTrue(false, "Expect IllegalStateException here!");
      } catch (IllegalStateException e) {
      }
      Assert.assertNull(client);

      ActorHandle<DummyServeController> actorHandle =
          Ray.actor(DummyServeController::new).setName(Constants.SERVE_CONTROLLER_NAME).remote();
      client = Serve.getGlobalClient();
      Assert.assertNotNull(client);
    } finally {
      if (!inited) {
        Ray.shutdown();
      }
    }
  }
}
