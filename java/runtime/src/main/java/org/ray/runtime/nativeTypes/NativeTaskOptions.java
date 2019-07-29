package org.ray.runtime.nativeTypes;

import org.ray.api.options.CallOptions;

public class NativeTaskOptions {
  public int numReturns;
  public NativeResources resources;

  public NativeTaskOptions(int numReturns, CallOptions options) {
    this.numReturns = numReturns;
    this.resources = new NativeResources(options == null ? null : options.resources);
  }
}
