package org.ray.runtime.nativeTypes;

import org.ray.api.id.ObjectId;
import org.ray.runtime.task.FunctionArg;

public class NativeTaskArg {
  public byte[] id;
  public byte[] data;

  public NativeTaskArg(ObjectId id, byte[] data) {
    if (id != null) {
      this.id = id.getBytes();
    } else {
      this.data = data;
    }
  }

  public NativeTaskArg(FunctionArg functionArg) {
    this(functionArg.id, functionArg.data);
  }
}
