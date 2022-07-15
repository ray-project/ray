package io.ray.serve.poll;

import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.serve.exception.RayServeException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class LongPollResult implements Serializable {

  private static final long serialVersionUID = 6603042077360718438L;

  Map<KeyType, UpdatedObject> updatedObjects;

  public Map<KeyType, UpdatedObject> getUpdatedObjects() {
    return updatedObjects;
  }

  public void setUpdatedObjects(Map<KeyType, UpdatedObject> updatedObjects) {
    this.updatedObjects = updatedObjects;
  }

  public static LongPollResult parseFrom(byte[] longPollResultBytes) {
    if (longPollResultBytes == null) {
      return null;
    }
    io.ray.serve.generated.LongPollResult pbLongPollResult = null;
    try {
      pbLongPollResult = io.ray.serve.generated.LongPollResult.parseFrom(longPollResultBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new RayServeException("Failed to parse LongPollResult from protobuf bytes.", e);
    }
    if (pbLongPollResult == null) {
      return null;
    }
    LongPollResult longPollResult = new LongPollResult();
    if (pbLongPollResult.getUpdatedObjectsMap() != null) {
      Map<KeyType, UpdatedObject> updatedObjects =
          new HashMap<>(pbLongPollResult.getUpdatedObjectsMap().size());
      for (Map.Entry<String, io.ray.serve.generated.UpdatedObject> entry :
          pbLongPollResult.getUpdatedObjectsMap().entrySet()) {
        KeyType keyType = KeyType.parseFrom(entry.getKey());
        updatedObjects.put(keyType, UpdatedObject.parseFrom(keyType, entry.getValue()));
      }
      longPollResult.setUpdatedObjects(updatedObjects);
    }
    return longPollResult;
  }
}
