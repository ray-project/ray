package io.ray.runtime.object;

import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.id.ObjectId;
import java.io.Serializable;

/**
 * Implementation of {@link ObjectRef}.
 */
public final class ObjectRefImpl<T> implements ObjectRef<T>, Serializable {

  private final ObjectId id;

  // In GC thread, we don't know which runtime this object binds to, so we need to store a reference
  // of the runtime for later uses.
  private transient AbstractRayRuntime runtime;

  private Class<T> type;

  public ObjectRefImpl(ObjectId id, Class<T> type) {
    this.id = id;
    this.type = type;
    addLocalReference();
  }

  @Override
  public synchronized T get() {
    return Ray.get(id, type);
  }

  @Override
  public ObjectId getId() {
    return id;
  }

  @Override
  public Class<T> getType() {
    return type;
  }

  @Override
  protected void finalize() throws Throwable {
    try {
      // Maybe the reference is already removed in unit test.
      if (runtime != null) {
        removeLocalReference();
      }
    } finally {
      super.finalize();
    }
  }

  private void readObject(java.io.ObjectInputStream in)
      throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    addLocalReference();
  }

  private void addLocalReference() {
    Preconditions.checkState(runtime == null);
    runtime = RuntimeUtil.getRuntime();
    Preconditions.checkState(!runtime.isShutdown(), "The runtime is already shutdown.");
    runtime.getObjectStore().addLocalReference(id);
  }

  // This method is public for test purposes only.
  public void removeLocalReference() {
    Preconditions.checkState(runtime != null);
    // It's possible that GC is executed after the runtime is shutdown.
    if (!runtime.isShutdown()) {
      runtime.getObjectStore().removeLocalReference(id);
    }
    runtime = null;
  }
}
