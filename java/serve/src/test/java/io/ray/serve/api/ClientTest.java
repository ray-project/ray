package io.ray.serve.api;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.serve.Constants;
import io.ray.serve.DummyServeController;
import io.ray.serve.RayServeConfig;
import io.ray.serve.RayServeHandle;
import io.ray.serve.generated.EndpointInfo;
import io.ray.serve.util.CommonUtil;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClientTest {

  @Test
  public void getHandleTest() {
    boolean inited = Ray.isInitialized();
    String previous_namespace = System.getProperty("ray.job.namespace");
    System.setProperty("ray.job.namespace", Constants.SERVE_NAMESPACE);
    Ray.init();

    try {
      String prefix = "ClientTest";
      String controllerName =
          CommonUtil.formatActorName(
              Constants.SERVE_CONTROLLER_NAME, RandomStringUtils.randomAlphabetic(6));
      String endpointName = prefix + "_endpoint";

      // Controller.
      ActorHandle<DummyServeController> controllerHandle =
          Ray.actor(DummyServeController::new).setName(controllerName).remote();

      // Set ReplicaContext
      Serve.setInternalReplicaContext(null, null, controllerName, null);
      Serve.getReplicaContext()
          .setRayServeConfig(
              new RayServeConfig().setConfig(RayServeConfig.LONG_POOL_CLIENT_ENABLED, "false"));

      // Mock endpoints.
      Map<String, EndpointInfo> endpoints = new HashMap<>();
      endpoints.put(endpointName, EndpointInfo.newBuilder().setEndpointName(endpointName).build());
      controllerHandle.task(DummyServeController::setEndpoints, endpoints).remote();

      // Client.
      Client client = new Client(controllerHandle, controllerName, true);

      // Get handle.
      RayServeHandle rayServeHandle = client.getHandle(endpointName, false);
      Assert.assertNotNull(rayServeHandle);
    } finally {
      if (!inited) {
        Ray.shutdown();
      }
      if (previous_namespace == null) {
        System.clearProperty("ray.job.namespace");
      } else {
        System.setProperty("ray.job.namespace", previous_namespace);
      }
      Serve.setInternalReplicaContext(null);
    }
  }
}
