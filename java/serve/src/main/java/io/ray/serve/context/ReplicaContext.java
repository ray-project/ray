package io.ray.serve.context;

import io.ray.serve.config.RayServeConfig;

/** Stores data for Serve API calls from within deployments. */
public class ReplicaContext {

  private String deploymentName;

  private String replicaTag;

  private String internalControllerName;

  private String internalControllerNamespace;

  private Object servableObject;

  private RayServeConfig rayServeConfig;

  public ReplicaContext(
      String deploymentName,
      String replicaTag,
      String controllerName,
      String controllerNamespace,
      Object servableObject) {
    this.deploymentName = deploymentName;
    this.replicaTag = replicaTag;
    this.internalControllerName = controllerName;
    this.internalControllerNamespace = controllerNamespace;
    this.servableObject = servableObject;
  }

  public String getDeploymentName() {
    return deploymentName;
  }

  public void setDeploymentName(String deploymentName) {
    this.deploymentName = deploymentName;
  }

  public String getReplicaTag() {
    return replicaTag;
  }

  public void setReplicaTag(String replicaTag) {
    this.replicaTag = replicaTag;
  }

  public String getInternalControllerName() {
    return internalControllerName;
  }

  public void setInternalControllerName(String internalControllerName) {
    this.internalControllerName = internalControllerName;
  }

  public String getInternalControllerNamespace() {
    return internalControllerNamespace;
  }

  public void setInternalControllerNamespace(String internalControllerNamespace) {
    this.internalControllerNamespace = internalControllerNamespace;
  }

  public Object getServableObject() {
    return servableObject;
  }

  public void setServableObject(Object servableObject) {
    this.servableObject = servableObject;
  }

  public RayServeConfig getRayServeConfig() {
    return rayServeConfig;
  }

  public void setRayServeConfig(RayServeConfig rayServeConfig) {
    this.rayServeConfig = rayServeConfig;
  }
}
