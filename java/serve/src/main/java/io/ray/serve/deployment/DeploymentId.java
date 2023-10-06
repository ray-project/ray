package io.ray.serve.deployment;

import org.apache.commons.lang3.StringUtils;

public class DeploymentId {
  private final String app;
  private final String name;

  public DeploymentId(String name, String app) {
    this.name = name;
    this.app = app;
  }

  public String toReplicaActorClassName() {
    if (StringUtils.isBlank(app)) {
      return "ServeReplica:" + name;
    } else {
      return "ServeReplica:" + app + ":" + name;
    }
  }

  public String getApp() {
    return app;
  }

  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    if (StringUtils.isBlank(app)) {
      return name;
    } else {
      return app + "_" + name;
    }
  }
}
