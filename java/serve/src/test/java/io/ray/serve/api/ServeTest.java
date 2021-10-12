package io.ray.serve.api;

import io.ray.serve.RayServeException;
import io.ray.serve.ReplicaContext;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ServeTest {

  @Test
  public void replicaContextTest() {

    ReplicaContext preContext = Serve.INTERNAL_REPLICA_CONTEXT;
    ReplicaContext replicaContext;

    // Test null replica context.
    Serve.INTERNAL_REPLICA_CONTEXT = null;
    try {
      replicaContext = Serve.getReplicaContext();
      Assert.assertTrue(false, "expect RayServeException");
    } catch (RayServeException e) {

    }

    // Test context setting and getting.
    String backendTag = "backendTag";
    String replicaTag = "replicaTag";
    String controllerName = "controllerName";
    Object servableObject = new Object();
    Serve.setInternalReplicaContext(backendTag, replicaTag, controllerName, servableObject);

    replicaContext = Serve.getReplicaContext();
    Assert.assertNotNull(replicaContext, "no replica context");
    Assert.assertEquals(replicaContext.getBackendTag(), backendTag);
    Assert.assertEquals(replicaContext.getReplicaTag(), replicaTag);
    Assert.assertEquals(replicaContext.getInternalControllerName(), controllerName);

    Serve.INTERNAL_REPLICA_CONTEXT = preContext;
  }
}
