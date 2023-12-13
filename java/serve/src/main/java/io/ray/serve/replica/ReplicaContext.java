package io.ray.serve.replica;

import java.util.Map;

/** Stores data for Serve API calls from within deployments. */
public class ReplicaContext {

  private String deploymentName;

  private String replicaTag;

  private Object servableObject;

  private Map<String, String> config;

  private String appName;

  public ReplicaContext(
      String deploymentName,
      String replicaTag,
      Object servableObject,
      Map<String, String> config,
      String appName) {
    this.deploymentName = deploymentName;
    this.replicaTag = replicaTag;
    this.servableObject = servableObject;
    this.config = config;
    this.appName = appName;
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

  public String getAppName() {
    return appName;
  }

  public void setAppName(String appName) {
    this.appName = appName;
  }
}
