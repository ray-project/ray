package io.ray.serve.api;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.serve.DummyServeController;
import io.ray.serve.common.Constants;
import io.ray.serve.config.RayServeConfig;
import io.ray.serve.generated.EndpointInfo;
import io.ray.serve.generated.EndpointSet;
import io.ray.serve.handle.RayServeHandle;
import io.ray.serve.poll.LongPollClientFactory;
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
      controllerHandle.task(DummyServeController::setEndpoints, endpointSet.toByteArray()).remote();

      // Client.
      ServeControllerClient client =
          new ServeControllerClient(controllerHandle, controllerName, true);

      // Get handle.
      RayServeHandle rayServeHandle = client.getHandle(endpointName, false);
      Assert.assertNotNull(rayServeHandle);
    } finally {
      LongPollClientFactory.stop();
      LongPollClientFactory.clearAllCache();
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
