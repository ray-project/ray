package io.ray.serve.api;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.serve.BaseServeTest;
import io.ray.serve.DummyServeController;
import io.ray.serve.common.Constants;
import io.ray.serve.config.RayServeConfig;
import io.ray.serve.generated.EndpointInfo;
import io.ray.serve.generated.EndpointSet;
import io.ray.serve.handle.RayServeHandle;
import io.ray.serve.util.CommonUtil;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ServeControllerClientTest {
  @Test
  public void getHandleTest() {
    BaseServeTest.initRay();
    try {
      String prefix = "ServeControllerClientTest";
      String controllerName = CommonUtil.formatActorName(Constants.SERVE_CONTROLLER_NAME, prefix);
      String endpointName = prefix + "_endpoint";
      Map<String, String> config = new HashMap<>();
      config.put(RayServeConfig.LONG_POOL_CLIENT_ENABLED, "false");

      // Controller.
      ActorHandle<DummyServeController> controllerHandle =
          Ray.actor(DummyServeController::new, "").setName(controllerName).remote();

      // Set ReplicaContext
      Serve.setInternalReplicaContext(null, null, controllerName, null, config);

      // Mock endpoints.
      EndpointSet endpointSet =
          EndpointSet.newBuilder()
              .putEndpoints(
                  endpointName, EndpointInfo.newBuilder().setEndpointName(endpointName).build())
              .build();
      controllerHandle
          .task(DummyServeController::setEndpoints, endpointSet.toByteArray())
          .remote()
          .get();

      // Client.
      ServeControllerClient client =
          new ServeControllerClient(controllerHandle, controllerName, true);

      // Get handle.
      RayServeHandle rayServeHandle = client.getHandle(endpointName, false);
      Assert.assertNotNull(rayServeHandle);
    } finally {
      BaseServeTest.clearAndShutdownRay();
    }
  }
}
