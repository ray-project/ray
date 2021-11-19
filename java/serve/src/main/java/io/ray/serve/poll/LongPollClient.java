package io.ray.serve.poll;

import com.google.common.base.Preconditions;
import io.ray.api.BaseActorHandle;
import java.util.Map;
import java.util.Set;

/** The asynchronous long polling client. */
public class LongPollClient {

  private Set<KeyType> keys;

  public LongPollClient(BaseActorHandle hostActor, Map<KeyType, KeyListener> keyListeners) {
    Preconditions.checkArgument(keyListeners != null && keyListeners.size() != 0);
    LongPollClientFactory.register(hostActor, keyListeners);
    this.keys = keyListeners.keySet();
  }

  @Override
  protected void finalize() throws Throwable {
    try {
      LongPollClientFactory.unregister(keys);
    } finally {
      super.finalize();
    }
  }
}
