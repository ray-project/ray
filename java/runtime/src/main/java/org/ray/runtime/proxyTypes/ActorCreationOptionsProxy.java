package org.ray.runtime.proxyTypes;

import org.ray.api.options.ActorCreationOptions;

public class ActorCreationOptionsProxy {
  public long maxReconstructions;
  public ResourcesProxy resources;


  public ActorCreationOptionsProxy(ActorCreationOptions options) {
    this.maxReconstructions = options == null ? ActorCreationOptions.NO_RECONSTRUCTION :
        options.maxReconstructions;
    this.resources = new ResourcesProxy(options == null ? null : options.resources);
  }
}
