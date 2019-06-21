package org.ray.runtime.proxyTypes;

import org.ray.api.id.ObjectId;
import org.ray.runtime.task.FunctionArg;

public class TaskArgProxy {
  public byte[] id;
  public byte[] data;

  public TaskArgProxy(ObjectId id, byte[] data) {
    if (id != null) {
      this.id = id.getBytes();
    } else {
      this.data = data;
    }
  }

  public TaskArgProxy(FunctionArg functionArg) {
    this(functionArg.id, functionArg.data);
  }
}
