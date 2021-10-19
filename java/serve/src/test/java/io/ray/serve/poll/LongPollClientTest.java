package io.ray.serve.poll;

import com.google.protobuf.ByteString;
import io.ray.serve.generated.BackendConfig;
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
    KeyType keyType = new KeyType(LongPollNamespace.BACKEND_CONFIGS, "backendTag");
    Map<KeyType, KeyListener> keyListeners = new HashMap<>();
    keyListeners.put(
        keyType, (object) -> a[0] = String.valueOf(((BackendConfig) object).getNumReplicas()));

    // Initialize LongPollClient.
    LongPollClient longPollClient = new LongPollClient(null, keyListeners);

    // Construct updated object.
    BackendConfig.Builder backendConfig = BackendConfig.newBuilder();
    backendConfig.setNumReplicas(20);
    int snapshotId = 10;
    UpdatedObject.Builder updatedObject = UpdatedObject.newBuilder();
    updatedObject.setSnapshotId(snapshotId);
    updatedObject.setObjectSnapshot(ByteString.copyFrom(backendConfig.build().toByteArray()));

    // Process update.
    Map<KeyType, UpdatedObject> updates = new HashMap<>();
    updates.put(keyType, updatedObject.build());
    longPollClient.processUpdate(updates);

    // Validation.
    Assert.assertEquals(longPollClient.getSnapshotIds().get(keyType).intValue(), snapshotId);
    Assert.assertEquals(
        ((BackendConfig) longPollClient.getObjectSnapshots().get(keyType)).getNumReplicas(),
        backendConfig.getNumReplicas());
    Assert.assertEquals(a[0], String.valueOf(backendConfig.getNumReplicas()));
  }
}
