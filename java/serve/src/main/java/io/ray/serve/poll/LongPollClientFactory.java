package io.ray.serve.poll;

import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.api.ActorHandle;
import io.ray.api.BaseActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.PyActorHandle;
import io.ray.api.Ray;
import io.ray.api.exception.RayActorException;
import io.ray.api.exception.RayTaskException;
import io.ray.api.function.PyActorMethod;
import io.ray.serve.Constants;
import io.ray.serve.RayServeConfig;
import io.ray.serve.RayServeException;
import io.ray.serve.ReplicaContext;
import io.ray.serve.ServeController;
import io.ray.serve.UpdatedObject;
import io.ray.serve.api.Serve;
import io.ray.serve.generated.ActorSet;
import io.ray.serve.util.CollectionUtil;
import io.ray.serve.util.LogUtil;
import io.ray.serve.util.ServeProtoUtil;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The long poll client factory that holds a asynchronous singleton thread. */
public class LongPollClientFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(LongPollClientFactory.class);

  /** Handle to actor embedding LongPollHost. */
  private static BaseActorHandle hostActor;

  /** A set mapping keys to callbacks to be called on state update for the corresponding keys. */
  private static final Map<KeyType, KeyListener> KEY_LISTENERS = new ConcurrentHashMap<>();

  public static final Map<KeyType, Integer> SNAPSHOT_IDS = new ConcurrentHashMap<>();

  public static final Map<KeyType, Object> OBJECT_SNAPSHOTS = new ConcurrentHashMap<>();

  private static ScheduledExecutorService scheduledExecutorService;

  private static boolean inited = false;

  public static final Map<LongPollNamespace, Function<byte[], Object>> DESERIALIZERS =
      new HashMap<>();

  static {
    DESERIALIZERS.put(LongPollNamespace.ROUTE_TABLE, body -> ServeProtoUtil.parseEndpointSet(body));
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

  public static void register(BaseActorHandle hostActor, Map<KeyType, KeyListener> keyListeners) {
    init(hostActor);
    if (!inited) {
      return;
    }
    KEY_LISTENERS.putAll(keyListeners);
    for (KeyType keyType : keyListeners.keySet()) {
      SNAPSHOT_IDS.put(keyType, -1);
    }
    LOGGER.info("LongPollClient registered keys: {}.", keyListeners.keySet());
  }

  public static synchronized void init(BaseActorHandle hostActor) {
    if (inited) {
      return;
    }

    ReplicaContext replicaContext = Serve.getReplicaContext();
    boolean enabled =
        Optional.ofNullable(replicaContext.getRayServeConfig())
            .map(rayServeConfig -> rayServeConfig.getConfig())
            .map(config -> config.get(RayServeConfig.LONG_POOL_CLIENT_ENABLED))
            .map(longPollClientEnabled -> Boolean.valueOf(longPollClientEnabled))
            .orElse(true);
    if (!enabled) {
      LOGGER.info("LongPollClient is disabled.");
      return;
    }

    LongPollClientFactory.hostActor =
        Optional.ofNullable(hostActor)
            .orElse(
                Ray.getActor(replicaContext.getInternalControllerName(), Constants.SERVE_NAMESPACE)
                    .get());

    scheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactory() {
              @Override
              public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "ray-serve-long-poll-client-thread");
                thread.setDaemon(true);
                return thread;
              }
            });
    long intervalS =
        Optional.ofNullable(replicaContext.getRayServeConfig())
            .map(rayServeConfig -> rayServeConfig.getConfig())
            .map(config -> config.get(RayServeConfig.LONG_POOL_CLIENT_INTERVAL))
            .map(longPollClientInterval -> Long.valueOf(longPollClientInterval))
            .orElse(10L);
    scheduledExecutorService.scheduleAtFixedRate(
        () -> {
          try {
            pollNext();
          } catch (RayActorException e) {
            LOGGER.error("LongPollClient failed to connect to host. Shutting down.");
            stop();
          } catch (RayTaskException e) {
            LOGGER.error("LongPollHost errored", e);
          } catch (Throwable e) {
            LOGGER.error("LongPollClient failed to update object of key {}", SNAPSHOT_IDS, e);
          }
        },
        intervalS,
        intervalS,
        TimeUnit.SECONDS);
    inited = true;
    LOGGER.info("LongPollClient was initialized with interval {}s.", intervalS);
  }

  /** Poll the updates. */
  @SuppressWarnings("unchecked")
  public static void pollNext() {
    LOGGER.info("LongPollClient polls next snapshotIds {}", SNAPSHOT_IDS);
    LongPollRequest longPollRequest = new LongPollRequest(SNAPSHOT_IDS);
    LongPollResult longPollResult = null;
    if (hostActor instanceof PyActorHandle) {
      // Poll from python controller.
      ObjectRef<Object> currentRef =
          ((PyActorHandle) hostActor)
              .task(
                  PyActorMethod.of(Constants.CONTROLLER_LISTEN_FOR_CHANGE_METHOD),
                  longPollRequest.toProtobuf().toByteArray())
              .remote();
      longPollResult = LongPollResult.parseFrom((byte[]) currentRef.get());
    } else {
      // Poll from java controller.
      ObjectRef<LongPollResult> currentRef =
          ((ActorHandle<ServeController>) hostActor)
              .task(ServeController::listenForChange, longPollRequest)
              .remote();
      longPollResult = currentRef.get();
    }
    processUpdate(longPollResult == null ? null : longPollResult.getUpdatedObjects());
  }

  public static void processUpdate(Map<KeyType, UpdatedObject> updates) {
    if (updates == null || updates.isEmpty()) {
      LOGGER.info("LongPollClient received nothing.");
      return;
    }
    LOGGER.info("LongPollClient received updates for keys: {}", updates.keySet());
    for (Map.Entry<KeyType, UpdatedObject> entry : updates.entrySet()) {
      KeyType keyType = entry.getKey();
      UpdatedObject updatedObject = entry.getValue();

      Object objectSnapshot = updatedObject.getObjectSnapshot();

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "The updated object for key {} is {}",
            keyType,
            ReflectionToStringBuilder.toString(objectSnapshot));
      }

      KeyListener keyListener = KEY_LISTENERS.get(entry.getKey());
      if (keyListener == null) {
        LOGGER.warn(
            "LongPollClient has no listener for key: {}, maybe this key was garbage collected.",
            entry.getKey());
        continue;
      }
      KEY_LISTENERS.get(entry.getKey()).notifyChanged(objectSnapshot);
      OBJECT_SNAPSHOTS.put(entry.getKey(), objectSnapshot);
      SNAPSHOT_IDS.put(entry.getKey(), entry.getValue().getSnapshotId());
    }
  }

  public static void unregister(Set<KeyType> keys) {
    if (CollectionUtil.isEmpty(keys)) {
      return;
    }
    for (KeyType keyType : keys) {
      SNAPSHOT_IDS.remove(keyType);
      KEY_LISTENERS.remove(keyType);
      OBJECT_SNAPSHOTS.remove(keyType);
    }
    LOGGER.info("LongPollClient unregistered keys: {}.", keys);
  }

  public static synchronized void stop() {
    if (!inited) {
      return;
    }
    if (scheduledExecutorService != null) {
      scheduledExecutorService.shutdown();
    }
    inited = false;
    LOGGER.info("LongPollClient was shopped.");
  }

  public static boolean isInitialized() {
    return inited;
  }
}
