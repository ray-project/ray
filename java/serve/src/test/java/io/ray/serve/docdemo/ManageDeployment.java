package io.ray.serve.docdemo;

import io.ray.serve.api.Serve;
import io.ray.serve.deployment.Deployment;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ManageDeployment {

  // create-start
  public static class Counter {

    private AtomicInteger value;

    public Counter(Integer value) {
      this.value = new AtomicInteger(value);
    }

    public String call(String delta) {
      return String.valueOf(value.addAndGet(Integer.valueOf(delta)));
    }
  }

  public void create() {
    Serve.deployment()
        .setName("counter")
        .setDeploymentDef(Counter.class.getName())
        .setInitArgs(new Object[] {1})
        .setNumReplicas(2)
        .create()
        .deploy(true);
  }
  // create-end

  // query_start
  public void query() {
    Deployment deployment = Serve.getDeployment("counter");
  }
  // query_end

  // __update_start__
  public void update() {
    Serve.deployment()
        .setName("counter")
        .setDeploymentDef(Counter.class.getName())
        .setInitArgs(new Object[] {2})
        .setNumReplicas(2)
        .create()
        .deploy(true);
  }
  // __update_end__

  // [scale-start]
  public void scaleOut() {
    Deployment deployment = Serve.getDeployment("counter");

    // Scale up to 10 replicas.
    deployment.options().setNumReplicas(10).create().deploy(true);

    // Scale down to 1 replica.
    deployment.options().setNumReplicas(1).create().deploy(true);
  }
  // [scale-end]

  // [resource-start]
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
  // [resource-end]
}
