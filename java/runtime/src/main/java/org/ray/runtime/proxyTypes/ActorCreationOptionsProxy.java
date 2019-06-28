package org.ray.runtime.proxyTypes;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.ray.api.options.ActorCreationOptions;
import org.ray.runtime.util.StringUtil;

public class ActorCreationOptionsProxy {
  public long maxReconstructions;
  public ResourcesProxy resources;
  public List<String> dynamicWorkerOptions;


  public ActorCreationOptionsProxy(ActorCreationOptions options) {
    this.maxReconstructions = options == null ? ActorCreationOptions.NO_RECONSTRUCTION :
        options.maxReconstructions;
    this.resources = new ResourcesProxy(options == null ? null : options.resources);
    this.dynamicWorkerOptions = ImmutableList.of();
    String jvmOptions = options.jvmOptions;
    if (!StringUtil.isNullOrEmpty(jvmOptions)) {
      this.dynamicWorkerOptions = ImmutableList.of(jvmOptions);
    }
  }
}
