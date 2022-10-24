package io.ray.serve.api;

import io.ray.api.PyActorHandle;
import io.ray.api.Ray;
import io.ray.serve.BaseServeTest;
import io.ray.serve.DummyServeController;
import io.ray.serve.common.Constants;
import io.ray.serve.config.RayServeConfig;
import io.ray.serve.exception.RayServeException;
import io.ray.serve.replica.ReplicaContext;
import io.ray.serve.replica.ReplicaName;
import io.ray.serve.util.CommonUtil;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

public class ServeTest {

  @Test
  public void getReplicaContextNormalTest() {
    try {
      String dummyName = "getReplicaContextNormalTest";
      ReplicaName replicaName = new ReplicaName(dummyName, RandomStringUtils.randomAlphabetic(6));
      String controllerName = dummyName;
      Object servableObject = new Object();
      Serve.setInternalReplicaContext(
          replicaName.getDeploymentTag(),
          replicaName.getReplicaTag(),
          controllerName,
          servableObject,
          null);

      ReplicaContext replicaContext = Serve.getReplicaContext();
      Assert.assertNotNull(replicaContext, "no replica context");
      Assert.assertEquals(replicaContext.getDeploymentName(), replicaName.getDeploymentTag());
      Assert.assertEquals(replicaContext.getReplicaTag(), replicaName.getReplicaTag());
      Assert.assertEquals(replicaContext.getInternalControllerName(), controllerName);
    } finally {
      BaseServeTest.clearContext();
    }
  }

  @Test
  public void getReplicaContextNotExistTest() {
    Assert.assertThrows(RayServeException.class, () -> Serve.getReplicaContext());
  }

  @Test(groups = {"cluster"})
  public void startTest() {
    try {
      // The default port 8000 is occupied by other processes on the ci platform.
      Map<String, String> config = Maps.newHashMap();
      config.put(RayServeConfig.PROXY_HTTP_PORT, "8341");
      Serve.start(true, false, config);

      Optional<PyActorHandle> controller = Ray.getActor(Constants.SERVE_CONTROLLER_NAME);
      Assert.assertTrue(controller.isPresent());

      Serve.shutdown();
      controller = Ray.getActor(Constants.SERVE_CONTROLLER_NAME);
      Assert.assertFalse(controller.isPresent());
    } finally {
      BaseServeTest.shutdownServe();
    }
  }

  @Test(groups = {"cluster"})
  public void getGlobalClientNormalTest() {
    try {
      BaseServeTest.startServe();

      ServeControllerClient client = Serve.getGlobalClient(true);
      Assert.assertNotNull(client);
    } finally {
      BaseServeTest.shutdownServe();
    }
  }

  @Test
  public void getGlobalClientExceptionTest() {
    Assert.assertThrows(IllegalStateException.class, () -> Serve.getGlobalClient());
  }

  @Test
  public void getGlobalClientInReplicaTest() {
    try {
      BaseServeTest.initRay();

      // Mock controller.
      String controllerName =
          CommonUtil.formatActorName(
              Constants.SERVE_CONTROLLER_NAME, "getGlobalClientInReplicaTest");
      Ray.actor(DummyServeController::new, "").setName(controllerName).remote();

      // Mock replica context.
      Serve.setInternalReplicaContext(null, null, controllerName, null, null);

      // Get client.
      ServeControllerClient client = Serve.getGlobalClient();
      Assert.assertNotNull(client);
    } finally {
      BaseServeTest.shutdownRay();
      BaseServeTest.clearContext();
    }
  }

  @Test(groups = {"cluster"})
  public void connectTest() {
    try {
      BaseServeTest.startServe();

      ServeControllerClient client = Serve.connect();
      Assert.assertNotNull(client);
    } finally {
      BaseServeTest.shutdownServe();
    }
  }
}
