package org.ray.deploy.rps.model.allocate;

import org.ray.deploy.rps.model.Request;

/**
 * alloc request model. new fields: affinity, size
 */
public class AllocateRequest extends Request {

  ContainerSpec containerSpec;
  String type;
  Integer size;

  public AllocateRequest() {
  }

  public ContainerSpec getContainerSpec() {
    return containerSpec;
  }

  public void setContainerSpec(ContainerSpec containerSpec) {
    this.containerSpec = containerSpec;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public void setSize(Integer size) {
    this.size = size;
  }

  @Override
  public String toString() {
    return "AllocateRequest{" +
        "containerSpec=" + containerSpec +
        ", type='" + type + '\'' +
        ", size=" + size +
        ", clusterName='" + clusterName + '\'' +
        ", sequence=" + sequence +
        '}';
  }
}
