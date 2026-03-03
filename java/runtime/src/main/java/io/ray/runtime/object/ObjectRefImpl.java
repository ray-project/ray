package io.ray.runtime.object;

import com.google.common.base.FinalizableReferenceQueue;
import com.google.common.base.FinalizableWeakReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.id.ObjectId;
import io.ray.runtime.AbstractRayRuntime;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation of {@link ObjectRef}. */
public final class ObjectRefImpl<T> implements ObjectRef<T>, Externalizable {

  private static final Logger LOG = LoggerFactory.getLogger(ObjectRefImpl.class);

  private static final FinalizableReferenceQueue REFERENCE_QUEUE = new FinalizableReferenceQueue();

  private static final Set<Reference<ObjectRefImpl<?>>> REFERENCES = Sets.newConcurrentHashSet();

  /// All the objects that are referenced by this worker.
  /// The key is the object ID in raw bytes, and the value is a weak reference to the ObjectRefImpl
  // object.
  private static ConcurrentHashMap<ObjectId, WeakReference<ObjectRefImpl<?>>> allObjects =
      new ConcurrentHashMap<>(1024);

  private ObjectId id;

  private Class<T> type;

  // Raw data of this object.
  // This is currently used by only the memory store objects.
  // This byte array object is generated in CoreWorkerMemoryStore::Put.
  private byte[] rawData = null;

  public ObjectRefImpl(ObjectId id, Class<T> type, boolean skipAddingLocalRef) {
    init(id, type, skipAddingLocalRef);
  }

  public ObjectRefImpl(ObjectId id, Class<T> type) {
    this(id, type, /*skipAddingLocalRef=*/ false);
  }

  public void init(ObjectId id, Class<?> type, boolean skipAddingLocalRef) {
    this.id = id;
    this.type = (Class<T>) type;
    AbstractRayRuntime runtime = (AbstractRayRuntime) Ray.internal();

    if (!skipAddingLocalRef) {
      runtime.getObjectStore().addLocalReference(id);
    }
    // We still add the reference so that the local ref count will be properly
    // decremented once this object is GCed.
    new ObjectRefImplReference(this);
  }

  private void setRawData(byte[] rawData) {
    Preconditions.checkState(this.rawData == null);
    this.rawData = rawData;
  }

  public ObjectRefImpl() {}

  @Override
  public synchronized T get() {
    return Ray.get(this);
  }

  @Override
  public synchronized T get(long timeoutMs) {
    return Ray.get(this, timeoutMs);
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
    AbstractRayRuntime runtime = (AbstractRayRuntime) Ray.internal();
    byte[] ownerAddress = runtime.getObjectStore().getOwnershipInfo(this.getId());
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

    AbstractRayRuntime runtime = (AbstractRayRuntime) Ray.internal();
    runtime.getObjectStore().addLocalReference(id);
    new ObjectRefImplReference(this);

    runtime
        .getObjectStore()
        .registerOwnershipInfoAndResolveFuture(
            this.id, ObjectSerializer.getOuterObjectId(), ownerAddress);
  }

  private static final class ObjectRefImplReference
      extends FinalizableWeakReference<ObjectRefImpl<?>> {

    private final ObjectId objectId;
    private final AtomicBoolean removed;

    public ObjectRefImplReference(ObjectRefImpl<?> obj) {
      super(obj, REFERENCE_QUEUE);
      this.objectId = obj.id;
      this.removed = new AtomicBoolean(false);
      REFERENCES.add(this);
    }

    @Override
    public void finalizeReferent() {
      // This method may be invoked multiple times on the same instance (due to explicit invoking in
      // unit tests).
      if (!removed.getAndSet(true)) {
        REFERENCES.remove(this);
        // It's possible that GC is executed after the runtime is shutdown.
        if (Ray.isInitialized()) {
          ((AbstractRayRuntime) (Ray.internal())).getObjectStore().removeLocalReference(objectId);
          allObjects.remove(objectId);
          LOG.debug("Object {} is finalized.", objectId);
        }
      }
    }
  }

  /// The callback that will be invoked once a Java object is allocated in memory store.
  private static void onMemoryStoreObjectAllocated(byte[] rawObjectId, byte[] data) {
    ObjectId objectId = new ObjectId(rawObjectId);
    Preconditions.checkState(rawObjectId != null);
    Preconditions.checkState(data != null);
    LOG.debug("onMemoryStoreObjectAllocated: {} , data.length is {}.", objectId, data.length);
    if (!allObjects.containsKey(objectId)) {
      LOG.info("The object {} doesn't exist in the weak reference pool", objectId);
      return;
    }
    WeakReference<ObjectRefImpl<?>> weakRef = allObjects.get(objectId);
    if (weakRef == null) {
      /// This will happen when a race condition occurs that at this moment,
      /// the item is being removed from map in another thread.
      LOG.info("The object {} has already been cleaned.", objectId);
      allObjects.remove(objectId);
      return;
    }
    ObjectRefImpl<?> objImpl = weakRef.get();
    if (objImpl == null) {
      LOG.info("The object {} has already been cleaned.", objectId);
      allObjects.remove(objectId);
    } else {
      // LOG.debug("The object {} set with a byte array data.", objectId);
      objImpl.setRawData(data);
    }
  }

  public static <T> void registerObjectRefImpl(ObjectId objectId, ObjectRefImpl<T> obj) {
    if (allObjects.containsKey(objectId)) {
      /// This is due to testLocalRefCounts() create 2 ObjectRefImpl objects for 1 id.
      LOG.warn("Duplicated object {}", objectId);
      return;
    }
    allObjects.put(objectId, new WeakReference<>(obj));
    LOG.debug("Putting object {} to weak reference pool.", objectId);
  }
}
