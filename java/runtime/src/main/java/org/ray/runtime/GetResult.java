package org.ray.runtime;

import org.ray.api.exception.RayException;

/**
 * A class that represents the result of a get operation.
 */
public class GetResult<T> {

  /**
   * Whether this object exists in object store.
   */
  public final boolean exists;

  /**
   * The Java object that was fetched and deserialized from the object store. Note, this field
   * only makes sense when @code{exists == true && exception !=null}.
   */
  public final T object;

  /**
   * If this field is not null, it represents the exception that occurred during object's creating
   * task.
   */
  public final RayException exception;

  GetResult(boolean exists, T object, RayException exception) {
    this.exists = exists;
    this.object = object;
    this.exception = exception;
  }
}
