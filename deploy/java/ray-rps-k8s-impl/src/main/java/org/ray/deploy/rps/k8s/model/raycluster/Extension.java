package org.ray.deploy.rps.k8s.model.raycluster;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import java.util.Arrays;
import java.util.Map;
import lombok.Data;

/**
 * extension spec for a ray cluster.
 * @see ./deploy/ray-operator/v1alpha1/raycluster_types.go
 */
@Data
@JsonDeserialize
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Extension implements KubernetesResource {

  private Integer replicas;
  private String type;
  private String image;
  private String groupName;
  private String command;
  private Map<String, String> labels;
  private Map<String, String> nodeSelector;
  private Affinity affinity;
  private ResourceRequirements resources;
  private Toleration[] tolerations;
  private EnvVar[] containerEnv;
  private String headServiceSuffix;
  private Map<String, String> annotations;
  private Volume[] volumes;
  private VolumeMount[] volumeMounts;

  @Override
  public String toString() {
    return "Extension{" +
        "replicas=" + replicas +
        ", type='" + type + '\'' +
        ", image='" + image + '\'' +
        ", groupName='" + groupName + '\'' +
        ", command='" + command + '\'' +
        ", labels=" + labels +
        ", nodeSelector=" + nodeSelector +
        ", affinity=" + affinity +
        ", resources=" + resources +
        ", tolerations=" + Arrays.toString(tolerations) +
        ", containerEnv=" + Arrays.toString(containerEnv) +
        ", headServiceSuffix='" + headServiceSuffix + '\'' +
        ", annotations=" + annotations +
        ", volumes=" + Arrays.toString(volumes) +
        ", volumeMounts=" + Arrays.toString(volumeMounts) +
        '}';
  }
}