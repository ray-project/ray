package org.ray.deploy.rps.k8s.model.raycluster;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import java.util.Arrays;
import lombok.Data;

/**
 * ray cluster spec for ray cluster in kubernetes
 * @see ./deploy/ray-operator/v1alpha1/raycluster_types.go
 */
@Data
@JsonDeserialize
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RayClusterSpec implements KubernetesResource {

  private String clusterName;
  private RayClusterImage images;
  private String imagePullPolicy;
  private Extension[] extensions;

  @Override
  public String toString() {
    return "RayClusterSpec{" +
        "clusterName='" + clusterName + '\'' +
        ", images=" + images +
        ", imagePullPolicy='" + imagePullPolicy + '\'' +
        ", extensions=" + Arrays.toString(extensions) +
        '}';
  }
}