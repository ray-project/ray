package org.ray.runtime.object;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.Serializable;
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
      if (!runtime.isShutdown()) {
        runtime.getObjectStore().removeLocalReference(id);
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
    runtime = RuntimeUtil.getRuntime();
    Preconditions.checkState(!runtime.isShutdown(), "The runtime is already shutdown.");
    runtime.getObjectStore().addLocalReference(id);
  }
}
