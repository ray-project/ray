package io.ray.serve.docdemo;

import com.google.common.collect.ImmutableMap;
import io.ray.serve.api.Serve;
import io.ray.serve.deployment.Deployment;

public class ManageDeployment {

  public class SimpleDeployment1 {

    public String call(String data) {
      return "SimpleDeployment1";
    }
  }

  public class SimpleDeployment2 {

    public String call(String data) {
      return "SimpleDeployment2";
    }
  }

  public void update() {
    Serve.deployment()
        .setName("my_deployment")
        .setDeploymentDef(SimpleDeployment1.class.getName())
        .setNumReplicas(1)
        .create()
        .deploy(true);

    Serve.deployment()
        .setName("my_deployment")
        .setDeploymentDef(SimpleDeployment2.class.getName())
        .setNumReplicas(1)
        .create()
        .deploy(true);
  }

  public void scaleOut() {
    // Create with a single replica.
    Deployment deployment =
        Serve.deployment()
            .setName("my_deployment")
            .setDeploymentDef(SimpleDeployment1.class.getName())
            .setNumReplicas(1)
            .create();
    deployment.deploy(true);

    // Scale up to 10 replicas.
    deployment.options().setNumReplicas(10).create().deploy(true);

    // Scale back down to 1 replica.
    deployment.options().setNumReplicas(1).create().deploy(true);
  }

  public void manageResouce() {
    Serve.deployment()
        .setName("my_deployment")
        .setDeploymentDef(SimpleDeployment1.class.getName())
        .setRayActorOptions(ImmutableMap.of("num_gpus", 1))
        .create()
        .deploy(true);
  }
}
