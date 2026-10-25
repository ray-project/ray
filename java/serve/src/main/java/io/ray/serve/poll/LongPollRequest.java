package io.ray.serve.poll;

import java.io.Serializable;
import java.util.Map;

public class LongPollRequest implements Serializable {

  private static final long serialVersionUID = 6700308335001848617L;

  private Map<KeyType, Integer> keysToSnapshotIds;

  public LongPollRequest(Map<KeyType, Integer> keysToSnapshotIds) {
    this.keysToSnapshotIds = keysToSnapshotIds;
  }

  public Map<KeyType, Integer> getKeysToSnapshotIds() {
    return keysToSnapshotIds;
  }

  public io.ray.serve.generated.LongPollRequest toProtobuf() {
    io.ray.serve.generated.LongPollRequest.Builder builder =
        io.ray.serve.generated.LongPollRequest.newBuilder();
    if (keysToSnapshotIds != null) {
      keysToSnapshotIds.entrySet().stream()
          .forEach(
              entry -> builder.putKeysToSnapshotIds(entry.getKey().toString(), entry.getValue()));
    }
    return builder.build();
  }
}
