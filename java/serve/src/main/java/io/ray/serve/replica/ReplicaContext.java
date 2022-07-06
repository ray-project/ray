package io.ray.serve.replica;

import java.util.Map;

/** Stores data for Serve API calls from within deployments. */
public class ReplicaContext {

  private String deploymentName;

  private String replicaTag;

  private String internalControllerName;

  private Object servableObject;

  private Map<String, String> config;

  public ReplicaContext(
      String deploymentName,
      String replicaTag,
      String controllerName,
      Object servableObject,
      Map<String, String> config) {
    this.deploymentName = deploymentName;
    this.replicaTag = replicaTag;
    this.internalControllerName = controllerName;
    this.servableObject = servableObject;
    this.config = config;
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

  public Object getServableObject() {
    return servableObject;
  }

  public void setServableObject(Object servableObject) {
    this.servableObject = servableObject;
  }

  public Map<String, String> getConfig() {
    return config;
  }

  public void setConfig(Map<String, String> config) {
    this.config = config;
  }
}
