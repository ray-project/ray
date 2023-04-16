package io.ray.serve.replica;

public interface RayServeReplica {

  Object handleRequest(Object requestMetadata, Object requestArgs);

  default Object reconfigure(Object userConfig) {
    return null;
  }

  default boolean checkHealth() {
    return true;
  }

  default boolean prepareForShutdown() {
    return true;
  }
}
