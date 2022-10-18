package io.ray.serve.poll;

import com.google.common.base.Preconditions;
import io.ray.api.ActorHandle;
import io.ray.api.BaseActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.PyActorHandle;
import io.ray.api.Ray;
import io.ray.api.exception.RayActorException;
import io.ray.api.exception.RayTaskException;
import io.ray.api.exception.RayTimeoutException;
import io.ray.api.function.PyActorMethod;
import io.ray.serve.api.Serve;
import io.ray.serve.common.Constants;
import io.ray.serve.config.RayServeConfig;
import io.ray.serve.controller.ServeController;
import io.ray.serve.generated.ActorNameList;
import io.ray.serve.replica.ReplicaContext;
import io.ray.serve.util.CollectionUtil;
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

  private static long longPollTimoutS = 10L;

  public static final Map<LongPollNamespace, Function<byte[], Object>> DESERIALIZERS =
      new HashMap<>();

  static {
    DESERIALIZERS.put(LongPollNamespace.ROUTE_TABLE, ServeProtoUtil::parseEndpointSet);
    DESERIALIZERS.put(
        LongPollNamespace.RUNNING_REPLICAS,
        bytes -> ServeProtoUtil.bytesToProto(bytes, ActorNameList::parseFrom));
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
    long intervalS = 6L;
    try {
      ReplicaContext replicaContext = Serve.getReplicaContext();
      boolean enabled =
          Optional.ofNullable(replicaContext.getConfig())
              .map(config -> config.get(RayServeConfig.LONG_POOL_CLIENT_ENABLED))
              .map(Boolean::valueOf)
              .orElse(true);
      if (!enabled) {
        LOGGER.info("LongPollClient is disabled.");
        return;
      }
      if (null == hostActor) {
        hostActor =
            Ray.getActor(replicaContext.getInternalControllerName(), Constants.SERVE_NAMESPACE)
                .get();
      }
      intervalS =
          Optional.ofNullable(replicaContext.getConfig())
              .map(config -> config.get(RayServeConfig.LONG_POOL_CLIENT_INTERVAL))
              .map(Long::valueOf)
              .orElse(1L);
      longPollTimoutS =
          Optional.ofNullable(replicaContext.getConfig())
              .map(config -> config.get(RayServeConfig.LONG_POOL_CLIENT_TIMEOUT_S))
              .map(Long::valueOf)
              .orElse(10L);
    } catch (Exception e) {
      LOGGER.info(
          "Serve.getReplicaContext()` may only be called from within a Ray Serve deployment.");
    }

    Preconditions.checkNotNull(hostActor);
    LongPollClientFactory.hostActor = hostActor;

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
    long finalIntervalS = intervalS;
    scheduledExecutorService.scheduleWithFixedDelay(
        () -> {
          try {
            pollNext();
          } catch (RayTimeoutException e) {
            LOGGER.info(
                "long poll timeout in {} seconds, execute next poll after {} seconds.",
                longPollTimoutS,
                finalIntervalS);
          } catch (RayActorException e) {
            LOGGER.error("LongPollClient failed to connect to host. Shutting down.");
            stop();
          } catch (RayTaskException e) {
            LOGGER.error("LongPollHost errored", e);
          } catch (Throwable e) {
            LOGGER.error("LongPollClient failed to update object of key {}", SNAPSHOT_IDS, e);
          }
        },
        0L,
        intervalS,
        TimeUnit.SECONDS);
    inited = true;
    LOGGER.info("LongPollClient was initialized");
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
      Object data = Ray.get(currentRef, longPollTimoutS * 1000);
      longPollResult = LongPollResult.parseFrom((byte[]) data);
    } else {
      // Poll from java controller.
      ObjectRef<byte[]> currentRef =
          ((ActorHandle<ServeController>) hostActor)
              .task(ServeController::listenForChange, longPollRequest)
              .remote();
      longPollResult = LongPollResult.parseFrom(currentRef.get(longPollTimoutS * 1000));
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
      try {
        scheduledExecutorService.awaitTermination(longPollTimoutS, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOGGER.error("awaitTermination error, the exception is ", e);
      }
    }
    KEY_LISTENERS.clear();
    OBJECT_SNAPSHOTS.clear();
    SNAPSHOT_IDS.clear();
    inited = false;
    LOGGER.info("LongPollClient was stopped.");
  }

  public static boolean isInitialized() {
    return inited;
  }
}
