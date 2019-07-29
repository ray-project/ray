package org.ray.runtime.nativeTypes;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.ray.api.options.ActorCreationOptions;
import org.ray.runtime.util.StringUtil;

public class NativeActorCreationOptions {
  public long maxReconstructions;
  public NativeResources resources;
  public List<String> dynamicWorkerOptions;


  public NativeActorCreationOptions(ActorCreationOptions options) {
    this.maxReconstructions = options == null ? ActorCreationOptions.NO_RECONSTRUCTION :
        options.maxReconstructions;
    this.resources = new NativeResources(options == null ? null : options.resources);
    this.dynamicWorkerOptions = ImmutableList.of();
    if (options != null && !StringUtil.isNullOrEmpty(options.jvmOptions)) {
      this.dynamicWorkerOptions = ImmutableList.of(options.jvmOptions);
    }
  }
}
