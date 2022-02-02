package io.ray.serve.poll;

import com.google.common.base.Preconditions;
import io.ray.api.BaseActorHandle;
import java.util.Map;

/** The asynchronous long polling client. */
public class LongPollClient {

  private Map<KeyType, KeyListener> keyListeners;

  public LongPollClient(BaseActorHandle hostActor, Map<KeyType, KeyListener> keyListeners) {
    Preconditions.checkArgument(keyListeners != null && keyListeners.size() != 0);
    LongPollClientFactory.register(hostActor, keyListeners);
    this.keyListeners = keyListeners;
  }

  public Map<KeyType, KeyListener> getKeyListeners() {
    return keyListeners;
  }
}
