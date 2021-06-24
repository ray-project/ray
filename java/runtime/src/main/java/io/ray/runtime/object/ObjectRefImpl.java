package io.ray.runtime.object;

import com.google.common.base.FinalizableReferenceQueue;
import com.google.common.base.FinalizableWeakReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.id.ObjectId;
import io.ray.api.id.UniqueId;
import io.ray.runtime.RayRuntimeInternal;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.ref.Reference;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/** Implementation of {@link ObjectRef}. */
public final class ObjectRefImpl<T> implements ObjectRef<T>, Externalizable {

  private static final FinalizableReferenceQueue REFERENCE_QUEUE = new FinalizableReferenceQueue();

  private static final Set<Reference<ObjectRefImpl<?>>> REFERENCES = Sets.newConcurrentHashSet();

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
    return Ray.get(this);
  }

  public ObjectId getId() {
    return id;
  }

  public Class<T> getType() {
    return type;
  }

  @Override
  public String toString() {
    return "ObjectRef(" + id.toString() + ")";
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(this.getId());
    out.writeObject(this.getType());
    RayRuntimeInternal runtime = (RayRuntimeInternal) Ray.internal();
    byte[] ownerAddress = runtime.getObjectStore().promoteAndGetOwnershipInfo(this.getId());
    out.writeInt(ownerAddress.length);
    out.write(ownerAddress);
    ObjectSerializer.addContainedObjectId(this.getId());
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    this.id = (ObjectId) in.readObject();
    this.type = (Class<T>) in.readObject();
    int len = in.readInt();
    byte[] ownerAddress = new byte[len];
    in.readFully(ownerAddress);
    addLocalReference();
    RayRuntimeInternal runtime = (RayRuntimeInternal) Ray.internal();
    runtime
        .getObjectStore()
        .registerOwnershipInfoAndResolveFuture(
            this.id, ObjectSerializer.getOuterObjectId(), ownerAddress);
  }

  private void addLocalReference() {
    Preconditions.checkState(workerId == null);
    RayRuntimeInternal runtime = (RayRuntimeInternal) Ray.internal();
    workerId = runtime.getWorkerContext().getCurrentWorkerId();
    runtime.getObjectStore().addLocalReference(workerId, id);
    new ObjectRefImplReference(this);
  }

  private static final class ObjectRefImplReference
      extends FinalizableWeakReference<ObjectRefImpl<?>> {

    private final UniqueId workerId;
    private final ObjectId objectId;
    private final AtomicBoolean removed;

    public ObjectRefImplReference(ObjectRefImpl<?> obj) {
      super(obj, REFERENCE_QUEUE);
      this.workerId = obj.workerId;
      this.objectId = obj.id;
      this.removed = new AtomicBoolean(false);
      REFERENCES.add(this);
    }

    @Override
    public void finalizeReferent() {
      // This method may be invoked multiple times on the same instance (due to explicit invoking in
      // unit tests). So if `workerId` is null, it means this method has been invoked.
      if (!removed.getAndSet(true)) {
        REFERENCES.remove(this);
        // It's possible that GC is executed after the runtime is shutdown.
        if (Ray.isInitialized()) {
          ((RayRuntimeInternal) (Ray.internal()))
              .getObjectStore()
              .removeLocalReference(workerId, objectId);
        }
      }
    }
  }
}
