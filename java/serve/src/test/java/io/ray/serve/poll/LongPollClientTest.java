package io.ray.serve.poll;

import com.google.protobuf.ByteString;
import io.ray.serve.generated.EndpointInfo;
import io.ray.serve.generated.EndpointSet;
import io.ray.serve.generated.UpdatedObject;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LongPollClientTest {

  @SuppressWarnings("unchecked")
  @Test
  public void test() throws Throwable {

    String[] a = new String[] {"test"};

    // Construct a listener map.
    KeyType keyType = new KeyType(LongPollNamespace.ROUTE_TABLE, null);
    Map<KeyType, KeyListener> keyListeners = new HashMap<>();
    keyListeners.put(
        keyType, (object) -> a[0] = String.valueOf(((Map<String, EndpointInfo>) object).size()));

    // Initialize LongPollClient.
    LongPollClient longPollClient = new LongPollClient(null, keyListeners);

    // Construct updated object.
    EndpointSet.Builder endpointSet = EndpointSet.newBuilder();
    endpointSet.putEndpoints("1", EndpointInfo.newBuilder().build());
    endpointSet.putEndpoints("2", EndpointInfo.newBuilder().build());
    int snapshotId = 10;
    UpdatedObject.Builder updatedObject = UpdatedObject.newBuilder();
    updatedObject.setSnapshotId(snapshotId);
    updatedObject.setObjectSnapshot(ByteString.copyFrom(endpointSet.build().toByteArray()));

    // Process update.
    Map<KeyType, UpdatedObject> updates = new HashMap<>();
    updates.put(keyType, updatedObject.build());
    longPollClient.processUpdate(updates);

    // Validation.
    Assert.assertEquals(longPollClient.getSnapshotIds().get(keyType).intValue(), snapshotId);
    Assert.assertEquals(
        ((Map<String, EndpointInfo>) longPollClient.getObjectSnapshots().get(keyType)).size(), 2);
    Assert.assertEquals(a[0], String.valueOf(endpointSet.getEndpointsMap().size()));
  }
}
