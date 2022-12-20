package io.ray.serve.deployment;

import io.ray.serve.generated.DeploymentStatus;

public class DeploymentStatusInfo {

  private DeploymentStatus deploymentStatus;

  private String message = "";

  public DeploymentStatusInfo(DeploymentStatus deploymentStatus, String message) {
    this.deploymentStatus = deploymentStatus;
    this.message = message;
  }

  public static DeploymentStatusInfo fromProto(io.ray.serve.generated.DeploymentStatusInfo proto) {
    return new DeploymentStatusInfo(proto.getStatus(), proto.getMessage());
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
