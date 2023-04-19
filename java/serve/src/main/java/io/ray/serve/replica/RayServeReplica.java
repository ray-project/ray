package io.ray.serve.replica;

import io.ray.serve.config.DeploymentConfig;

public interface RayServeReplica {

  Object handleRequest(Object requestMetadata, Object requestArgs);

  default Object reconfigure(DeploymentConfig deploymentConfig) {
    return null;
  }

  default boolean checkHealth() {
    return true;
  }

  default boolean prepareForShutdown() {
    return true;
  }
}
