package io.ray.serve;

import java.io.Serializable;
import java.util.Map;

/**
 * ReplicaConfig.
 */
public class ReplicaConfig implements Serializable {

  private String backendDef;

  private Object[] initArgs;

  private Map<String, Object> rayActorOptions;

  private Map<String, Object> resource;

  public ReplicaConfig(String backendDef, Object[] initArgs, Map<String, Object> rayActorOptions) {

  }

}
