package io.ray.serve;

import java.io.Serializable;

public class DeploymentInfo implements Serializable {

  private static final long serialVersionUID = -4198364411759931955L;

  private byte[] backendConfig;

  private ReplicaConfig replicaConfig;

  private byte[] backendVersion;

  public byte[] getBackendConfig() {
    return backendConfig;
  }

  public void setBackendConfig(byte[] backendConfig) {
    this.backendConfig = backendConfig;
  }

  public ReplicaConfig getReplicaConfig() {
    return replicaConfig;
  }

  public void setReplicaConfig(ReplicaConfig replicaConfig) {
    this.replicaConfig = replicaConfig;
  }

  public byte[] getDeploymentVersion() {
    return backendVersion;
  }

  public void setDeploymentVersion(byte[] backendVersion) {
    this.backendVersion = backendVersion;
  }
}
