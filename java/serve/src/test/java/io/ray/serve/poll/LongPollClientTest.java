package io.ray.serve.poll;

import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LongPollClientTest {

  @Test
  public void test() throws Throwable {

    KeyType keyType = new KeyType(LongPollNamespace.BACKEND_CONFIGS, "backendTag");
    int[] a = new int[] {0};
    Map<KeyType, KeyListener> keyListeners = new HashMap<>();
    keyListeners.put(keyType, (object) -> a[0] = (Integer) object);
    LongPollClient longPollClient = new LongPollClient(null, keyListeners);

    int snapshotId = 10;
    int objectSnapshot = 20;
    UpdatedObject updatedObject = new UpdatedObject();
    updatedObject.setSnapshotId(snapshotId);
    updatedObject.setObjectSnapshot(objectSnapshot);

    Map<KeyType, UpdatedObject> updates = new HashMap<>();
    updates.put(keyType, updatedObject);
    longPollClient.processUpdate(updates);

    Assert.assertEquals(longPollClient.getSnapshotIds().get(keyType).intValue(), snapshotId);
    Assert.assertEquals(
        ((Integer) longPollClient.getObjectSnapshots().get(keyType)).intValue(), objectSnapshot);
    Assert.assertEquals(a[0], objectSnapshot);
  }
}
