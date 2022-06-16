package io.ray.serve.api;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.serve.BaseTest;
import io.ray.serve.DummyServeController;
import io.ray.serve.common.Constants;
import io.ray.serve.replica.ReplicaContext;
import io.ray.serve.util.CommonUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ServeTest extends BaseTest {

  @Test
  public void replicaContextTest() {

    try {
      // Test context setting and getting.
      String deploymentName = "deploymentName";
      String replicaTag = "replicaTag";
      String controllerName = "controllerName";
      Object servableObject = new Object();
      Serve.setInternalReplicaContext(
          deploymentName, replicaTag, controllerName, servableObject, null);

      ReplicaContext replicaContext = Serve.getReplicaContext();
      Assert.assertNotNull(replicaContext, "no replica context");
      Assert.assertEquals(replicaContext.getDeploymentName(), deploymentName);
      Assert.assertEquals(replicaContext.getReplicaTag(), replicaTag);
      Assert.assertEquals(replicaContext.getInternalControllerName(), controllerName);
    } finally {
      clear();
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void getGlobalClientTest() {
    init();
    try {
      ServeControllerClient client = null;
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
          Ray.actor(DummyServeController::new, "", "").setName(controllerName).remote();
      Serve.setInternalReplicaContext(null, null, controllerName, null, null);
      client = Serve.getGlobalClient();
      Assert.assertNotNull(client);
    } finally {
      shutdown();
    }
  }
}
