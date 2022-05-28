package io.ray.serve.poll;

import java.util.Map;

import com.google.common.base.Preconditions;

import io.ray.api.BaseActorHandle;

/** The asynchronous long polling client. */
public class LongPollClient {

  private Map<KeyType, KeyListener> keyListeners;

  private boolean running;

  public LongPollClient(BaseActorHandle hostActor, Map<KeyType, KeyListener> keyListeners) {
    Preconditions.checkArgument(keyListeners != null && keyListeners.size() != 0);
    LongPollClientFactory.register(hostActor, keyListeners);
    this.keyListeners = keyListeners;
    this.running = true;
  }

  public Map<KeyType, KeyListener> getKeyListeners() {
    return keyListeners;
  }

  public boolean isRunning() {
    return running;
  }
}
