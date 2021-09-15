package io.ray.serve.api;

import io.ray.serve.ReplicaContext;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ServeTest {

  @Test
  public void replicaContextTest() {

    ReplicaContext preContext = Serve.getReplicaContext();
    ReplicaContext replicaContext;

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

    Serve.setInternalReplicaContext(
        preContext.getBackendTag(),
        preContext.getReplicaTag(),
        preContext.getInternalControllerName(),
        preContext.getServableObject());
  }
}
