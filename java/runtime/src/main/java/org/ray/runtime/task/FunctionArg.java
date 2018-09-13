package org.ray.runtime.task;

import org.ray.api.id.UniqueId;

/**
 * Represents arguments for ray function calls.
 */
public class FunctionArg {

  public final UniqueId id;
  public final byte[] data;

  public FunctionArg(UniqueId id, byte[] data) {
    this.id = id;
    this.data = data;
  }

  public void toString(StringBuilder builder) {
    builder.append("ids: ").append(id).append(", ").append("<data>:").append(data);
  }
}
