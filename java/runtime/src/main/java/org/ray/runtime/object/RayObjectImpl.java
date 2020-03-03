package org.ray.runtime.object;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.nustaq.serialization.FSTBasicObjectSerializer;
import org.nustaq.serialization.FSTClazzInfo;
import org.nustaq.serialization.FSTObjectOutput;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.id.ObjectId;
import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.util.RuntimeUtil;

/**
 * Implementation of {@link RayObject}.
 */
public final class RayObjectImpl<T> implements RayObject<T>, Serializable {

  private final ObjectId id;

  // In GC thread, we don't know which runtime this object binds to, so we need to store a reference
  // of the runtime for later uses.
  private transient AbstractRayRuntime runtime;

  public RayObjectImpl(ObjectId id) {
    this.id = id;
    addLocalReference();
  }

  @Override
  public synchronized T get() {
    return Ray.get(id);
  }

  @Override
  public ObjectId getId() {
    return id;
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

  public static class Serializer extends FSTBasicObjectSerializer {

    static ThreadLocal<Set<ObjectId>> innerIds = ThreadLocal.withInitial(HashSet::new);

    @Override
    public void writeObject(FSTObjectOutput out, Object toWrite, FSTClazzInfo clzInfo, FSTClazzInfo.FSTFieldInfo referencedBy, int streamPosition) throws IOException {
      out.writeObject(toWrite);
      RayObjectImpl object = (RayObjectImpl) toWrite;
      innerIds.get().add(object.getId());
    }

    public static List<ObjectId> getInnerObjectIds() {
      List<ObjectId> ids = new ArrayList<>();
      ids.addAll(innerIds.get());
      innerIds.get().clear();
      return ids;
    }
  }
}
