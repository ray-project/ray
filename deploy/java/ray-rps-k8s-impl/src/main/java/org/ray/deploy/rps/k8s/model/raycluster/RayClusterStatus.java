package org.ray.deploy.rps.k8s.model.raycluster;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import lombok.Data;

@Data
@JsonDeserialize
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RayClusterStatus implements KubernetesResource {

}