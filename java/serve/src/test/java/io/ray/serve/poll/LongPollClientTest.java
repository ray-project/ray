package io.ray.serve.poll;

import com.google.protobuf.ByteString;
import io.ray.serve.DeploymentConfig;
import io.ray.serve.generated.UpdatedObject;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LongPollClientTest {

  @Test
  public void test() throws Throwable {

    String[] a = new String[] {"test"};

    // Construct a listener map.
    KeyType keyType = new KeyType(LongPollNamespace.BACKEND_CONFIGS, "deploymentName");
    Map<KeyType, KeyListener> keyListeners = new HashMap<>();
    keyListeners.put(
        keyType, (object) -> a[0] = String.valueOf(((DeploymentConfig) object).getNumReplicas()));

    // Initialize LongPollClient.
    LongPollClient longPollClient = new LongPollClient(null, keyListeners);

    // Construct updated object.
    io.ray.serve.generated.DeploymentConfig.Builder deploymentConfig =
        io.ray.serve.generated.DeploymentConfig.newBuilder();
    deploymentConfig.setNumReplicas(20);
    int snapshotId = 10;
    UpdatedObject.Builder updatedObject = UpdatedObject.newBuilder();
    updatedObject.setSnapshotId(snapshotId);
    updatedObject.setObjectSnapshot(ByteString.copyFrom(deploymentConfig.build().toByteArray()));

    // Process update.
    Map<KeyType, UpdatedObject> updates = new HashMap<>();
    updates.put(keyType, updatedObject.build());
    longPollClient.processUpdate(updates);

    // Validation.
    Assert.assertEquals(longPollClient.getSnapshotIds().get(keyType).intValue(), snapshotId);
    Assert.assertEquals(
        ((DeploymentConfig) longPollClient.getObjectSnapshots().get(keyType)).getNumReplicas(),
        deploymentConfig.getNumReplicas());
    Assert.assertEquals(a[0], String.valueOf(deploymentConfig.getNumReplicas()));
  }
}
