package io.ray.serve.replica;

public class ReplicaName {

  private String deploymentTag;

  private String replicaSuffix;

  private String replicaTag = "";

  private String delimiter = "#";

  public ReplicaName(String deploymentTag, String replicaSuffix) {
    this.deploymentTag = deploymentTag;
    this.replicaSuffix = replicaSuffix;
    this.replicaTag = deploymentTag + this.delimiter + replicaSuffix;
  }

  public String getDeploymentTag() {
    return deploymentTag;
  }

  public String getReplicaSuffix() {
    return replicaSuffix;
  }

  public String getReplicaTag() {
    return replicaTag;
  }
}
