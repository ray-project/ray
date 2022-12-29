package io.ray.serve.docdemo;

import io.ray.serve.api.Serve;
import io.ray.serve.deployment.Deployment;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ManageDeployment {

  // docs-create-start
  public static class Counter {

    private AtomicInteger value;

    public Counter(String value) {
      this.value = new AtomicInteger(Integer.valueOf(value));
    }

    public String call(String delta) {
      return String.valueOf(value.addAndGet(Integer.valueOf(delta)));
    }
  }

  public void create() {
    Serve.deployment()
        .setName("counter")
        .setDeploymentDef(Counter.class.getName())
        .setInitArgs(new Object[] {"1"})
        .setNumReplicas(1)
        .create()
        .deploy(true);
  }
  // docs-create-end

  // docs-query-start
  public Deployment query() {
    Deployment deployment = Serve.getDeployment("counter");
    return deployment;
  }
  // docs-query-end

  // docs-update-start
  public void update() {
    Serve.deployment()
        .setName("counter")
        .setDeploymentDef(Counter.class.getName())
        .setInitArgs(new Object[] {"2"})
        .setNumReplicas(1)
        .create()
        .deploy(true);
  }
  // docs-update-end

  // docs-scale-start
  public void scaleOut() {
    Deployment deployment = Serve.getDeployment("counter");

    // Scale up to 2 replicas.
    deployment.options().setNumReplicas(2).create().deploy(true);

    // Scale down to 1 replica.
    deployment.options().setNumReplicas(1).create().deploy(true);
  }
  // docs-scale-end

  // docs-resource-start
  public void manageResource() {
    Map<String, Object> rayActorOptions = new HashMap<>();
    rayActorOptions.put("num_gpus", 1);
    Serve.deployment()
        .setName("counter")
        .setDeploymentDef(Counter.class.getName())
        .setRayActorOptions(rayActorOptions)
        .create()
        .deploy(true);
  }
  // docs-resource-end
}
