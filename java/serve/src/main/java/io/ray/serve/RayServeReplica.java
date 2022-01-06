package io.ray.serve;

public interface RayServeReplica {

  Object handleRequest(Object requestMetadata, Object requestArgs);

  Object reconfigure(Object userConfig);

  boolean checkHealth();

  boolean prepareForShutdown();
}
