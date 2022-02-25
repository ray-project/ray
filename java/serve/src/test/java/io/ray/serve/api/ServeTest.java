package io.ray.serve.api;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.serve.Constants;
import io.ray.serve.DummyServeController;
import io.ray.serve.ReplicaContext;
import io.ray.serve.util.CommonUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ServeTest {

  @Test
  public void replicaContextTest() {

    try {
      // Test context setting and getting.
      String deploymentName = "deploymentName";
      String replicaTag = "replicaTag";
      String controllerName = "controllerName";
      Object servableObject = new Object();
      Serve.setInternalReplicaContext(deploymentName, replicaTag, controllerName, servableObject);

      ReplicaContext replicaContext = Serve.getReplicaContext();
      Assert.assertNotNull(replicaContext, "no replica context");
      Assert.assertEquals(replicaContext.getDeploymentName(), deploymentName);
      Assert.assertEquals(replicaContext.getReplicaTag(), replicaTag);
      Assert.assertEquals(replicaContext.getInternalControllerName(), controllerName);
    } finally {
      // Recover context.
      Serve.setInternalReplicaContext(null);
    }
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

      String controllerName =
          CommonUtil.formatActorName(
              Constants.SERVE_CONTROLLER_NAME, RandomStringUtils.randomAlphabetic(6));
      ActorHandle<DummyServeController> actorHandle =
          Ray.actor(DummyServeController::new).setName(controllerName).remote();
      Serve.setInternalReplicaContext(null, null, controllerName, null);
      client = Serve.getGlobalClient();
      Assert.assertNotNull(client);
    } finally {
      if (!inited) {
        Ray.shutdown();
      }
      Serve.setInternalReplicaContext(null);
      Serve.setGlobalClient(null);
    }
  }
}
