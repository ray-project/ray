package org.ray.deploy.rps.k8s.model.raycluster;

import io.fabric8.kubernetes.api.builder.Function;
import io.fabric8.kubernetes.client.CustomResourceDoneable;

public class DoneableRaycluster extends CustomResourceDoneable<RayCluster> {

  public DoneableRaycluster(RayCluster resource,
      Function<RayCluster, RayCluster> function) {
    super(resource, function);
  }
}