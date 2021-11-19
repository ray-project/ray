package io.ray.serve;

import io.ray.api.BaseActorHandle;
import java.io.Serializable;

@SuppressWarnings("serial")
public class RunningReplicaInfo implements Serializable {

  private String deploymentName;

  private String replicaTag;

  private BaseActorHandle actorHandle;

  private int maxConcurrentQueries;

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

  public BaseActorHandle getActorHandle() {
    return actorHandle;
  }

  public void setActorHandle(BaseActorHandle actorHandle) {
    this.actorHandle = actorHandle;
  }

  public int getMaxConcurrentQueries() {
    return maxConcurrentQueries;
  }

  public void setMaxConcurrentQueries(int maxConcurrentQueries) {
    this.maxConcurrentQueries = maxConcurrentQueries;
  }
}
