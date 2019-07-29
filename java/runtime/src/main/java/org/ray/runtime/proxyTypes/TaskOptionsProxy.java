package org.ray.runtime.proxyTypes;

import org.ray.api.options.CallOptions;

public class TaskOptionsProxy {
  public int numReturns;
  public ResourcesProxy resources;

  public TaskOptionsProxy(int numReturns, CallOptions options) {
    this.numReturns = numReturns;
    this.resources = new ResourcesProxy(options == null ? null : options.resources);
  }
}
