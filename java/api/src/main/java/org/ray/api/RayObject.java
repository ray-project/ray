package org.ray.api;

import java.io.Serializable;
import org.ray.util.exception.TaskExecutionException;

/**
 * RayObject&lt;T&gt; is a handle for T object, which may or may not be available now.
 * That said, RayObject can be viewed as a Future object with metadata.
 */
public class RayObject<T> implements Serializable {

  private static final long serialVersionUID = 3250003902037418062L;

  UniqueID id;

  public RayObject() {
  }

  public RayObject(UniqueID id) {
    this.id = id;
  }

  public T get() throws TaskExecutionException {
    return Ray.get(id);
  }

  public <TM> TM getMeta() throws TaskExecutionException {
    return Ray.getMeta(id);
  }

  public UniqueID getId() {
    return id;
  }

}
