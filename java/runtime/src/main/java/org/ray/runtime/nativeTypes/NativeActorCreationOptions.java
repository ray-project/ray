package org.ray.runtime.nativeTypes;

import org.ray.api.options.ActorCreationOptions;

public class NativeActorCreationOptions {
  public long maxReconstructions;
  public NativeResources resources;


  public NativeActorCreationOptions(ActorCreationOptions options) {
    this.maxReconstructions = options == null ? ActorCreationOptions.NO_RECONSTRUCTION :
        options.maxReconstructions;
    this.resources = new NativeResources(options == null ? null : options.resources);
  }
}
