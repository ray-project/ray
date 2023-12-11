package io.ray.serve.api;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.serve.BaseServeTest;
import io.ray.serve.DummyServeController;
import io.ray.serve.common.Constants;
import io.ray.serve.config.RayServeConfig;
import io.ray.serve.generated.EndpointInfo;
import io.ray.serve.generated.EndpointSet;
import io.ray.serve.handle.DeploymentHandle;
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
      String endpointName = prefix + "_endpoint";
      Map<String, String> config = new HashMap<>();
      config.put(RayServeConfig.LONG_POOL_CLIENT_ENABLED, "false");

      // Controller.
      ActorHandle<DummyServeController> controllerHandle =
          Ray.actor(DummyServeController::new, "")
              .setName(Constants.SERVE_CONTROLLER_NAME)
              .remote();

      // Set ReplicaContext
      Serve.setInternalReplicaContext(null, null, null, config, null);

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
      ServeControllerClient client = new ServeControllerClient(controllerHandle);

      // Get handle.
      DeploymentHandle handle = client.getDeploymentHandle(endpointName, "", false);
      Assert.assertNotNull(handle);
    } finally {
      BaseServeTest.clearAndShutdownRay();
    }
  }
}
