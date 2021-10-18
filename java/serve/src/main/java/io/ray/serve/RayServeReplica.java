package io.ray.serve;

public interface RayServeReplica {

  Object handleRequest(Object requestMetadata, Object requestArgs);

  public Object reconfigure(Object userConfig);

  public boolean checkHealth();

  public boolean prepareForShutdown();
}
