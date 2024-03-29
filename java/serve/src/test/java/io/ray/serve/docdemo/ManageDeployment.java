package io.ray.serve.docdemo;

import io.ray.serve.api.Serve;
import io.ray.serve.deployment.Application;
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
    Application app =
        Serve.deployment()
            .setName("counter")
            .setDeploymentDef(Counter.class.getName())
            .setNumReplicas(1)
            .bind("1");
    Serve.run(app);
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
    Application app =
        Serve.deployment()
            .setName("counter")
            .setDeploymentDef(Counter.class.getName())
            .setNumReplicas(1)
            .bind("2");
    Serve.run(app);
  }
  // docs-update-end

  // docs-scale-start
  public void scaleOut() {
    Deployment deployment = Serve.getDeployment("counter");

    // Scale up to 2 replicas.
    Serve.run(deployment.options().setNumReplicas(2).bind());

    // Scale down to 1 replica.
    Serve.run(deployment.options().setNumReplicas(1).bind());
  }
  // docs-scale-end

  // docs-resource-start
  public void manageResource() {
    Map<String, Object> rayActorOptions = new HashMap<>();
    rayActorOptions.put("num_gpus", 1);
    Application app =
        Serve.deployment()
            .setName("counter")
            .setDeploymentDef(Counter.class.getName())
            .setRayActorOptions(rayActorOptions)
            .bind();
    Serve.run(app);
  }
  // docs-resource-end
}
