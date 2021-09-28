package io.ray.serve.poll;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.api.BaseActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.PyActorHandle;
import io.ray.api.function.PyActorMethod;
import io.ray.runtime.exception.RayActorException;
import io.ray.runtime.exception.RayTaskException;
import io.ray.serve.Constants;
import io.ray.serve.RayServeException;
import io.ray.serve.generated.ActorSet;
import io.ray.serve.generated.UpdatedObject;
import io.ray.serve.util.LogUtil;
import io.ray.serve.util.ServeProtoUtil;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The asynchronous long polling client. */
public class LongPollClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(LongPollClient.class);

  /** Handle to actor embedding LongPollHost. */
  private BaseActorHandle hostActor;

  /** A set mapping keys to callbacks to be called on state update for the corresponding keys. */
  private Map<KeyType, KeyListener> keyListeners;

  private Map<KeyType, Integer> snapshotIds;

  private Map<KeyType, Object> objectSnapshots;

  private ObjectRef<Object> currentRef;

  /** An async thread to post the callback into. */
  private Thread pollThread;

  private static final Map<LongPollNamespace, Function<byte[], Object>> DESERIALIZERS =
      new HashMap<>();

  static {
    DESERIALIZERS.put(
        LongPollNamespace.BACKEND_CONFIGS, body -> ServeProtoUtil.parseBackendConfig(body));
    DESERIALIZERS.put(
        LongPollNamespace.REPLICA_HANDLES, body -> ServeProtoUtil.parseEndpointSet(body));
    DESERIALIZERS.put(
        LongPollNamespace.REPLICA_HANDLES,
        body -> {
          try {
            return ActorSet.parseFrom(body);
          } catch (InvalidProtocolBufferException e) {
            throw new RayServeException(
                LogUtil.format("Failed to parse ActorSet from protobuf bytes."), e);
          }
        });
  }

  public LongPollClient(BaseActorHandle hostActor, Map<KeyType, KeyListener> keyListeners) {

    Preconditions.checkArgument(keyListeners != null && keyListeners.size() != 0);

    this.hostActor = hostActor;
    this.keyListeners = keyListeners;
    this.snapshotIds = new ConcurrentHashMap<>();
    for (KeyType keyType : keyListeners.keySet()) {
      this.snapshotIds.put(keyType, -1);
    }
    this.objectSnapshots = new ConcurrentHashMap<>();
    this.pollThread =
        new Thread(
            () -> {
              while (true) {
                try {
                  pollNext();
                } catch (RayActorException e) {
                  LOGGER.error("LongPollClient failed to connect to host. Shutting down.");
                  break;
                } catch (RayTaskException e) {
                  LOGGER.error("LongPollHost errored", e);
                } catch (Throwable e) {
                  LOGGER.error("LongPollClient failed to update object of key {}", snapshotIds, e);
                }
              }
            },
            "backend-poll-thread");
  }

  public void start() {
    if (!(hostActor instanceof PyActorHandle)) {
      LOGGER.warn("LongPollClient only support Python controller now.");
      return;
    }
    pollThread.start();
  }

  /**
   * Poll the update.
   *
   * @throws InvalidProtocolBufferException if the protobuf deserialization fails.
   */
  public void pollNext() throws InvalidProtocolBufferException {
    currentRef =
        ((PyActorHandle) hostActor)
            .task(PyActorMethod.of(Constants.CONTROLLER_LISTEN_FOR_CHANGE_METHOD), snapshotIds)
            .remote();
    processUpdate(ServeProtoUtil.parseUpdatedObjects((byte[]) currentRef.get()));
  }

  public void processUpdate(Map<KeyType, UpdatedObject> updates) {
    if (updates == null || updates.isEmpty()) {
      LOGGER.info("LongPollClient received nothing.");
      return;
    }
    LOGGER.info("LongPollClient received updates for keys: {}", updates.keySet());
    for (Map.Entry<KeyType, UpdatedObject> entry : updates.entrySet()) {
      KeyType keyType = entry.getKey();
      UpdatedObject updatedObject = entry.getValue();

      Object objectSnapshot =
          DESERIALIZERS
              .get(keyType.getLongPollNamespace())
              .apply(updatedObject.getObjectSnapshot().toByteArray());

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "The updated object for key {} is {}",
            keyType,
            ReflectionToStringBuilder.toString(objectSnapshot));
      }

      keyListeners.get(entry.getKey()).notifyChanged(objectSnapshot);
      objectSnapshots.put(entry.getKey(), objectSnapshot);
      snapshotIds.put(entry.getKey(), entry.getValue().getSnapshotId());
    }
  }

  public Map<KeyType, Integer> getSnapshotIds() {
    return snapshotIds;
  }

  public Map<KeyType, Object> getObjectSnapshots() {
    return objectSnapshots;
  }
}
