package io.ray.serve.poll;

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

/**
 * The asynchronous long polling client.
 */
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

  private boolean running;

  public LongPollClient(BaseActorHandle hostActor, Map<KeyType, KeyListener> keyListeners) {
    this.hostActor = hostActor;
    this.keyListeners = keyListeners;
    reset();
    this.running = true;
    this.pollThread = new Thread(() -> {
      while (running) {
        try {
          pollNext();
        } catch (Throwable e) {
          LOGGER.error("Long poll client failed to poll updated object of key {}", snapshotIds, e);
        }
      }
    }, "backend-poll-thread");
    this.pollThread.start();
  }

  private void reset() {
    snapshotIds = new ConcurrentHashMap<>();
    objectSnapshots = new ConcurrentHashMap<>();
    currentRef = null;
  }

  /**
   * Poll the update.
   * 
   * @throws Throwable if exception happens
   */
  private void pollNext() throws Throwable {
    currentRef = ((PyActorHandle) hostActor)
        .task(PyActorMethod.of(Constants.CONTROLLER_LISTEN_FOR_CHANGE_METHOD), snapshotIds)
        .remote();
    Object updates = currentRef.get();
    processUpdate(updates);

  }

  @SuppressWarnings("unchecked")
  private void processUpdate(Object updates) throws Throwable {
    if (updates instanceof RayActorException) {
      LOGGER.debug("LongPollClient failed to connect to host. Shutting down.");
      running = false;
      return;
    }

    if (updates instanceof RayTaskException) {
      LOGGER.error("LongPollHost errored", (RayTaskException) updates);
      return;
    }

    Map<KeyType, UpdatedObject> updateObjects = (Map<KeyType, UpdatedObject>) updates;

    LOGGER.debug("LongPollClient received updates for keys: {}", updateObjects.keySet());
    
    for (Map.Entry<KeyType, UpdatedObject> entry : updateObjects.entrySet()) {
      objectSnapshots.put(entry.getKey(), entry.getValue().getObjectSnapshot());
      snapshotIds.put(entry.getKey(), entry.getValue().getSnapshotId());
      KeyListener keyListener = keyListeners.get(entry.getKey());
      keyListener.notifyChanged(entry.getValue().getObjectSnapshot());
    }

  }

}
