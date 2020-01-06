package org.ray.deploy.rps.k8s.model.raycluster;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Data;
import org.ray.deploy.rps.k8s.model.BaseCustomResource;

/**
 * kubernetes custom resource for a ray cluster.
 * @see ./deploy/ray-operator/v1alpha1/raycluster_types.go
 */
@Data
@JsonDeserialize
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RayCluster extends BaseCustomResource {

  private RayClusterSpec spec;
  private RayClusterStatus status;

  @Override
  public String toString() {
    return "RayCluster{" +
        "spec=" + spec +
        ", status=" + status +
        '}';
  }
}