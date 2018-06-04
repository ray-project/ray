package org.ray.spi.model;

import java.util.ArrayList;
import org.ray.api.UniqueID;

/**
 * Represents arguments for ray function calls.
 */
public class FunctionArg {

  public ArrayList<UniqueID> ids;
  public byte[] data;

  public void toString(StringBuilder builder) {
    builder.append("ids: ").append(ids).append(", ").append("<data>:").append(data);
  }
}
