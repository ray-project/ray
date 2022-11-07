package io.ray.serve.poll;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.serve.BaseServeTest;
import io.ray.serve.DummyServeController;
import io.ray.serve.api.Serve;
import io.ray.serve.common.Constants;
import io.ray.serve.config.RayServeConfig;
import io.ray.serve.generated.EndpointInfo;
import io.ray.serve.generated.EndpointSet;
import io.ray.serve.generated.LongPollResult;
import io.ray.serve.generated.UpdatedObject;
import io.ray.serve.replica.ReplicaContext;
import io.ray.serve.util.CommonUtil;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LongPollClientTest {
  @Test
  public void disableTest() throws Throwable {
    Map<String, String> config = new HashMap<>();
    config.put(RayServeConfig.LONG_POOL_CLIENT_ENABLED, "false");

    ReplicaContext replicaContext = new ReplicaContext(null, null, null, null, config);
    Serve.setInternalReplicaContext(replicaContext);
    try {
      LongPollClientFactory.init(null);
      Assert.assertFalse(LongPollClientFactory.isInitialized());
    } finally {
      Serve.setInternalReplicaContext(null);
    }
  }

  @SuppressWarnings({"unchecked", "unused"})
  @Test
  public void normalTest() throws Throwable {
    BaseServeTest.initRay();
    try {
      String prefix = "LongPollClientTest_normalTest";
      // Init controller.
      String controllerName = CommonUtil.formatActorName(Constants.SERVE_CONTROLLER_NAME, prefix);
      ActorHandle<DummyServeController> controllerHandle =
          Ray.actor(DummyServeController::new, "").setName(controllerName).remote();

      Serve.setInternalReplicaContext(null, null, controllerName, null, null);

      // Init route table.
      String endpointName1 = prefix + "_endpoint1";
      String endpointName2 = prefix + "_endpoint2";
      EndpointSet endpointSet =
          EndpointSet.newBuilder()
              .putEndpoints(
                  endpointName1, EndpointInfo.newBuilder().setEndpointName(endpointName1).build())
              .putEndpoints(
                  endpointName2, EndpointInfo.newBuilder().setEndpointName(endpointName2).build())
              .build();

      // Construct a listener map.
      KeyType keyType = new KeyType(LongPollNamespace.ROUTE_TABLE, null);
      String[] testData = new String[] {"test"};
      Map<KeyType, KeyListener> keyListeners = new HashMap<>();
      keyListeners.put(
          keyType,
          (object) ->
              testData[0] =
                  ((Map<String, EndpointInfo>) object).get(endpointName1).getEndpointName());

      // Register.
      LongPollClient longPollClient = new LongPollClient(controllerHandle, keyListeners);
      Assert.assertTrue(LongPollClientFactory.isInitialized());

      // Construct updated object.
      int snapshotId = 10;
      UpdatedObject updatedObject =
          UpdatedObject.newBuilder()
              .setSnapshotId(snapshotId)
              .setObjectSnapshot(endpointSet.toByteString())
              .build();

      // Mock LongPollResult.
      LongPollResult longPollResult =
          LongPollResult.newBuilder().putUpdatedObjects(keyType.toString(), updatedObject).build();
      ObjectRef<Boolean> mockLongPollResult =
          controllerHandle
              .task(DummyServeController::setLongPollResult, longPollResult.toByteArray())
              .remote();
      Assert.assertEquals(mockLongPollResult.get().booleanValue(), true);

      // Poll.
      LongPollClientFactory.pollNext();

      // Validation.
      Assert.assertEquals(LongPollClientFactory.SNAPSHOT_IDS.get(keyType).intValue(), snapshotId);
      Assert.assertEquals(
          ((Map<String, EndpointInfo>) LongPollClientFactory.OBJECT_SNAPSHOTS.get(keyType)).size(),
          2);
      Assert.assertEquals(testData[0], endpointName1);

      LongPollClientFactory.stop();
      Assert.assertFalse(LongPollClientFactory.isInitialized());
    } finally {
      BaseServeTest.clearAndShutdownRay();
    }
  }
}
