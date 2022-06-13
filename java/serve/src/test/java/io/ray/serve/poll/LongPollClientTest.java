package io.ray.serve.poll;

import com.google.common.collect.ImmutableMap;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.serve.Constants;
import io.ray.serve.DummyServeController;
import io.ray.serve.RayServeConfig;
import io.ray.serve.ReplicaContext;
import io.ray.serve.UpdatedObject;
import io.ray.serve.api.Serve;
import io.ray.serve.generated.EndpointInfo;
import io.ray.serve.util.CommonUtil;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LongPollClientTest {

  @Test
  public void disableTest() throws Throwable {
    ReplicaContext replicaContext = new ReplicaContext(null, null, null, null);
    replicaContext.setRayServeConfig(
        new RayServeConfig().setConfig(RayServeConfig.LONG_POOL_CLIENT_ENABLED, "false"));
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
    boolean inited = Ray.isInitialized();
    String previous_namespace = System.getProperty("ray.job.namespace");
    System.setProperty("ray.job.namespace", Constants.SERVE_NAMESPACE);
    Ray.init();

    try {
      // Init controller.
      String controllerName =
          CommonUtil.formatActorName(
              Constants.SERVE_CONTROLLER_NAME, RandomStringUtils.randomAlphabetic(6));
      ActorHandle<DummyServeController> controllerHandle =
          Ray.actor(DummyServeController::new).setName(controllerName).remote();

      Serve.setInternalReplicaContext(null, null, controllerName, null);

      // Init route table.
      String endpointName1 = "normalTest1";
      String endpointName2 = "normalTest2";
      Map<String, EndpointInfo> endpoints = new HashMap<>();
      endpoints.put(
          endpointName1, EndpointInfo.newBuilder().setEndpointName(endpointName1).build());
      endpoints.put(
          endpointName2, EndpointInfo.newBuilder().setEndpointName(endpointName2).build());

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
      LongPollClient longPollClient = new LongPollClient(null, keyListeners);
      Assert.assertTrue(LongPollClientFactory.isInitialized());

      // Construct updated object.
      int snapshotId = 10;
      UpdatedObject updatedObject = new UpdatedObject();
      updatedObject.setSnapshotId(snapshotId);
      updatedObject.setObjectSnapshot(endpoints);

      // Mock LongPollResult.
      LongPollResult longPollResult = new LongPollResult();
      longPollResult.setUpdatedObjects(ImmutableMap.of(keyType, updatedObject));
      ObjectRef<Boolean> mockLongPollResult =
          controllerHandle.task(DummyServeController::setLongPollResult, longPollResult).remote();
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
      if (!inited) {
        Ray.shutdown();
      }
      if (previous_namespace == null) {
        System.clearProperty("ray.job.namespace");
      } else {
        System.setProperty("ray.job.namespace", previous_namespace);
      }
      Serve.setInternalReplicaContext(null);
      LongPollClientFactory.stop();
    }
  }
}
