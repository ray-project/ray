package io.ray.runtime.object;

import com.google.common.base.Preconditions;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.id.ObjectId;
import io.ray.api.id.UniqueId;
import io.ray.runtime.RayRuntimeInternal;
import java.io.IOException;
import java.io.Serializable;

/**
 * Implementation of {@link ObjectRef}.
 */
public final class ObjectRefImpl<T> implements ObjectRef<T>, Serializable {

  private final ObjectId id;

  // In GC thread, we don't know which worker this object binds to, so we need to
  // store the worker ID for later uses.
  private transient UniqueId workerId;

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
      removeLocalReference();
    } finally {
      super.finalize();
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    addLocalReference();
  }

  private void addLocalReference() {
    Preconditions.checkState(workerId == null);
    RayRuntimeInternal runtime = (RayRuntimeInternal) Ray.internal();
    workerId = runtime.getWorkerContext().getCurrentWorkerId();
    runtime.getObjectStore().addLocalReference(workerId, id);
  }

  // This method is public for test purposes only.
  public void removeLocalReference() {
    Preconditions.checkState(workerId != null);
    RayRuntimeInternal runtime = (RayRuntimeInternal) Ray.internal();
    // It's possible that GC is executed after the runtime is shutdown.
    if (runtime != null) {
      runtime.getObjectStore().removeLocalReference(workerId, id);
    }
    runtime = null;
  }
}
