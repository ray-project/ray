package org.ray.deploy.rps.model;

/**
 * elasticity container id.
 */
public class ElasticityContainer {

  String containerId;

  public ElasticityContainer(String containerId) {
    this.containerId = containerId;
  }

  public String getContainerId() {
    return containerId;
  }

  public void setContainerId(String containerId) {
    this.containerId = containerId;
  }

  @Override
  public String toString() {
    return "ElasticityContainer{" +
        "containerId='" + containerId + '\'' +
        '}';
  }
}
