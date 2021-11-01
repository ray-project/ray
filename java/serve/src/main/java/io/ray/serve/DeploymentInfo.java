package io.ray.serve;

import java.io.Serializable;

public class DeploymentInfo implements Serializable {

  private static final long serialVersionUID = -4198364411759931955L;

  private byte[] deploymentConfig;

  private ReplicaConfig replicaConfig;

  private byte[] deploymentVersion;

  public byte[] getDeploymentConfig() {
    return deploymentConfig;
  }

  public void setDeploymentConfig(byte[] deploymentConfig) {
    this.deploymentConfig = deploymentConfig;
  }

  public ReplicaConfig getReplicaConfig() {
    return replicaConfig;
  }

  public void setReplicaConfig(ReplicaConfig replicaConfig) {
    this.replicaConfig = replicaConfig;
  }

  public byte[] getDeploymentVersion() {
    return deploymentVersion;
  }

  public void setDeploymentVersion(byte[] deploymentVersion) {
    this.deploymentVersion = deploymentVersion;
  }
}
