package io.ray.serve.deployment;

import io.ray.serve.config.DeploymentConfig;
import io.ray.serve.config.ReplicaConfig;

public class DeploymentInfo {

  private String name;

  private DeploymentConfig deploymentConfig;

  private ReplicaConfig replicaConfig;

  private Long startTimeMs;

  private String actorName;

  private String version;

  private Long endTimeMs;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public DeploymentConfig getDeploymentConfig() {
    return deploymentConfig;
  }

  public void setDeploymentConfig(DeploymentConfig deploymentConfig) {
    this.deploymentConfig = deploymentConfig;
  }

  public ReplicaConfig getReplicaConfig() {
    return replicaConfig;
  }

  public void setReplicaConfig(ReplicaConfig replicaConfig) {
    this.replicaConfig = replicaConfig;
  }

  public Long getStartTimeMs() {
    return startTimeMs;
  }

  public void setStartTimeMs(Long startTimeMs) {
    this.startTimeMs = startTimeMs;
  }

  public String getActorName() {
    return actorName;
  }

  public void setActorName(String actorName) {
    this.actorName = actorName;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public Long getEndTimeMs() {
    return endTimeMs;
  }

  public void setEndTimeMs(Long endTimeMs) {
    this.endTimeMs = endTimeMs;
  }

  public static DeploymentInfo fromProto(io.ray.serve.generated.DeploymentInfo proto) {

    if (proto == null) {
      return null;
    }
    DeploymentInfo deploymentInfo = new DeploymentInfo();
    deploymentInfo.setName(proto.getName());
    deploymentInfo.setDeploymentConfig(DeploymentConfig.fromProto(proto.getDeploymentConfig()));
    deploymentInfo.setReplicaConfig(ReplicaConfig.fromProto(proto.getReplicaConfig()));
    if (proto.getStartTimeMs() != 0) {
      deploymentInfo.setStartTimeMs(proto.getStartTimeMs());
    }
    deploymentInfo.setActorName(proto.getActorName());
    deploymentInfo.setVersion(proto.getVersion());
    if (proto.getEndTimeMs() != 0) {
      deploymentInfo.setEndTimeMs(proto.getEndTimeMs());
    }
    return deploymentInfo;
  }
}
