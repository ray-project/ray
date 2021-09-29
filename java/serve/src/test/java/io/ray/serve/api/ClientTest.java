package io.ray.serve.api;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.serve.DummyServeController;
import io.ray.serve.RayServeHandle;
import io.ray.serve.generated.EndpointInfo;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClientTest {

  @Test
  public void getHandleTest() {

    boolean inited = Ray.isInitialized();
    Ray.init();

    try {
      String prefix = "ClientTest";
      String controllerName = prefix + "_controller";
      String endpointName = prefix + "_endpoint";

      // Controller.
      ActorHandle<DummyServeController> controllerHandle =
          Ray.actor(DummyServeController::new).setName(controllerName).remote();

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
    }
  }
}
