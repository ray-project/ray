package io.ray.serve.deployment;

import io.ray.serve.generated.DeploymentStatus;

public class DeploymentStatusInfo {

  private String name;

  private DeploymentStatus deploymentStatus;

  private String message = "";

  public DeploymentStatusInfo(String name, DeploymentStatus deploymentStatus, String message) {
    this.name = name;
    this.deploymentStatus = deploymentStatus;
    this.message = message;
  }

  public static DeploymentStatusInfo fromProto(io.ray.serve.generated.DeploymentStatusInfo proto) {
    return new DeploymentStatusInfo(proto.getName(), proto.getStatus(), proto.getMessage());
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public DeploymentStatus getDeploymentStatus() {
    return deploymentStatus;
  }

  public void setDeploymentStatus(DeploymentStatus deploymentStatus) {
    this.deploymentStatus = deploymentStatus;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }
}
