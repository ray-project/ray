package org.ray.deploy.rps.k8s.model.raycluster;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import lombok.Data;

/**
 * @see ./deploy/ray-operator/v1alpha1/raycluster_types.go
 */
@Data
@JsonDeserialize
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RayClusterImage implements KubernetesResource {

  private String defaultImage;
  private String clusterApiServerImage;

  @Override
  public String toString() {
    return "RayClusterImage{" +
        "defaultImage='" + defaultImage + '\'' +
        ", clusterApiServerImage='" + clusterApiServerImage + '\'' +
        '}';
  }
}