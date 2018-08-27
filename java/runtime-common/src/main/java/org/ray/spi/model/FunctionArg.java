package org.ray.spi.model;

import java.util.ArrayList;
import org.ray.api.UniqueID;

/**
 * Represents arguments for ray function calls.
 */
public class FunctionArg {

  public final UniqueID id;
  public final byte[] data;

  public FunctionArg(UniqueID id, byte[] data) {
    this.id = id;
    this.data = data;
  }

  public void toString(StringBuilder builder) {
    builder.append("ids: ").append(id).append(", ").append("<data>:").append(data);
  }
}
