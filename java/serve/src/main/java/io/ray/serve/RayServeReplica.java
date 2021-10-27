package io.ray.serve;

public interface RayServeReplica {

  Object handleRequest(Object requestMetadata, Object requestArgs);

  default Object reconfigure(Object userConfig) {
    return new BackendVersion(null, userConfig);
  }

  default boolean checkHealth() {
    return true;
  }

  default boolean prepareForShutdown() {
    return true;
  }
}
