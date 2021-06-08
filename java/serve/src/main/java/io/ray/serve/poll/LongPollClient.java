package io.ray.serve.poll;

import com.google.common.base.Preconditions;
import io.ray.api.BaseActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.PyActorHandle;
import io.ray.api.function.PyActorMethod;
import io.ray.runtime.exception.RayActorException;
import io.ray.runtime.exception.RayTaskException;
import io.ray.serve.Constants;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
                  LOGGER.debug("LongPollClient failed to connect to host. Shutting down.");
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

  /** Poll the update. */
  @SuppressWarnings("unchecked")
  public void pollNext() {
    currentRef =
        ((PyActorHandle) hostActor)
            .task(PyActorMethod.of(Constants.CONTROLLER_LISTEN_FOR_CHANGE_METHOD), snapshotIds)
            .remote();
    processUpdate((Map<KeyType, UpdatedObject>) currentRef.get());
  }

  public void processUpdate(Map<KeyType, UpdatedObject> updates) {

    LOGGER.debug("LongPollClient received updates for keys: {}", updates.keySet());

    for (Map.Entry<KeyType, UpdatedObject> entry : updates.entrySet()) {
      objectSnapshots.put(entry.getKey(), entry.getValue().getObjectSnapshot());
      snapshotIds.put(entry.getKey(), entry.getValue().getSnapshotId());
      keyListeners.get(entry.getKey()).notifyChanged(entry.getValue().getObjectSnapshot());
    }
  }

  public Map<KeyType, Integer> getSnapshotIds() {
    return snapshotIds;
  }

  public Map<KeyType, Object> getObjectSnapshots() {
    return objectSnapshots;
  }
}
