package io.ray.runtime.object;

import com.google.common.base.Preconditions;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.id.ObjectId;
import io.ray.api.id.UniqueId;
import io.ray.runtime.RayRuntimeInternal;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Implementation of {@link ObjectRef}.
 */
public final class ObjectRefImpl<T> implements ObjectRef<T>, Externalizable {

  private ObjectId id;

  // In GC thread, we don't know which worker this object binds to, so we need to
  // store the worker ID for later uses.
  private transient UniqueId workerId;

  private Class<T> type;

  public ObjectRefImpl(ObjectId id, Class<T> type) {
    this.id = id;
    this.type = type;
    addLocalReference();
  }

  public ObjectRefImpl() {}

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

  static ThreadLocal<Set<ObjectId>> containedObjectIds = ThreadLocal.withInitial(HashSet::new);

  public static List<ObjectId> getAndClearContainedObjectIds() {
    List<ObjectId> ids = new ArrayList<>(containedObjectIds.get());
    containedObjectIds.get().clear();
    return ids;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(this.getId());
    out.writeObject(this.getType());
    containedObjectIds.get().add(this.getId());
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    this.id = (ObjectId) in.readObject();
    this.type = (Class<T>) in.readObject();
    addLocalReference();
  }

  private void addLocalReference() {
    Preconditions.checkState(workerId == null);
    RayRuntimeInternal runtime = (RayRuntimeInternal) Ray.internal();
    workerId = runtime.getWorkerContext().getCurrentWorkerId();
    runtime.getObjectStore().addLocalReference(workerId, id);
  }

  // This method is public for test purposes only.
  public synchronized void removeLocalReference() {
    if (workerId != null) {
      RayRuntimeInternal runtime = (RayRuntimeInternal) Ray.internal();
      // It's possible that GC is executed after the runtime is shutdown.
      if (runtime != null) {
        runtime.getObjectStore().removeLocalReference(workerId, id);
      }
      workerId = null;
    }
  }
}
