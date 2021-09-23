package io.ray.serve;

/** Stores data for Serve API calls from within the user's backend code. */
public class ReplicaContext {

  private String DeploymentTag;

  private String replicaTag;

  private String internalControllerName;

  private Object servableObject;

  public ReplicaContext(
      String DeploymentTag, String replicaTag, String controllerName, Object servableObject) {
    this.DeploymentTag = DeploymentTag;
    this.replicaTag = replicaTag;
    this.internalControllerName = controllerName;
    this.servableObject = servableObject;
  }

  public String getDeploymentTag() {
    return DeploymentTag;
  }

  public void setDeploymentTag(String DeploymentTag) {
    this.DeploymentTag = DeploymentTag;
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
}
