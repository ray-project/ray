package org.ray.runtime.task;

import org.ray.api.id.UniqueId;

/**
 * Represents a function argument in task spec.
 *
 * Either `id` or `data` should be null, when id is not null, this argument will be
 * passed by reference, otherwise it will be passed by value.
 */
public class FunctionArg {

  /**
   * The id of this argument (passed by reference).
   */
  public final UniqueId id;
  /**
   * Serialized data of this argument (passed by value).
   */
  public final byte[] data;

  public FunctionArg(UniqueId id, byte[] data) {
    this.id = id;
    this.data = data;
  }

  @Override
  public String toString() {
    if (id != null) {
      return "<id>: " + id.toString();
    } else {
      return "<data>: " + data.length;
    }
  }
}
