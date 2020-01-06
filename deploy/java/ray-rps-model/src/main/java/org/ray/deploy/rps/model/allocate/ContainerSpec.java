package org.ray.deploy.rps.model.allocate;

import org.ray.deploy.rps.model.ResourceDescription;

/**
 * resource description of a group of elasticity containers
 */
public class ContainerSpec {

  ResourceDescription resourceDescription;

  public ContainerSpec() {
  }

  public ResourceDescription getResourceDescription() {
    return resourceDescription;
  }

  public void setResourceDescription(ResourceDescription resourceDescription) {
    this.resourceDescription = resourceDescription;
  }

  @Override
  public String toString() {
    return "ContainerSpec{" +
        "resourceDescription=" + resourceDescription +
        '}';
  }
}
