package io.ray.serve;

public interface RayServeReplica {

  Object handleRequest(Object requestMetadata, Object requestArgs);

  default Object reconfigure(Object userConfig) {
    return new DeploymentVersion(null, userConfig);
  }

  default boolean checkHealth() {
    return true;
  }

  default boolean prepareForShutdown() {
    return true;
  }
}
