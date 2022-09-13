package io.ray.serve.poll;

import io.ray.serve.exception.RayServeException;
import java.io.Serializable;
import java.util.function.Function;

public class UpdatedObject implements Serializable {

  private static final long serialVersionUID = 4079205733405537177L;

  private int snapshotId;

  private Object objectSnapshot;

  public int getSnapshotId() {
    return snapshotId;
  }

  public void setSnapshotId(int snapshotId) {
    this.snapshotId = snapshotId;
  }

  public Object getObjectSnapshot() {
    return objectSnapshot;
  }

  public void setObjectSnapshot(Object objectSnapshot) {
    this.objectSnapshot = objectSnapshot;
  }

  public static UpdatedObject parseFrom(
      KeyType keyType, io.ray.serve.generated.UpdatedObject pbUpdatedObject) {
    if (pbUpdatedObject == null) {
      return null;
    }
    UpdatedObject updatedObject = new UpdatedObject();
    updatedObject.setSnapshotId(pbUpdatedObject.getSnapshotId());

    Function<byte[], Object> deserializer =
        LongPollClientFactory.DESERIALIZERS.get(keyType.getLongPollNamespace());
    if (deserializer == null) {
      throw new RayServeException(
          "No deserializer for LongPollNamespace: " + keyType.getLongPollNamespace());
    }
    updatedObject.setObjectSnapshot(
        deserializer.apply(pbUpdatedObject.getObjectSnapshot().toByteArray()));
    return updatedObject;
  }
}
