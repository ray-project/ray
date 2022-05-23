package io.ray.serve;

import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ReplicaConfigTest {

  static interface Validator {
    void validate();
  }

  @Test
  public void test() {

    Object dummy = new Object();
    String deploymentDef = "io.ray.serve.ReplicaConfigTest";

    expectIllegalArgumentException(
        () -> new ReplicaConfig(deploymentDef, null, getRayActorOptions("placement_group", dummy)));

    expectIllegalArgumentException(
        () -> new ReplicaConfig(deploymentDef, null, getRayActorOptions("lifetime", dummy)));

    expectIllegalArgumentException(
        () -> new ReplicaConfig(deploymentDef, null, getRayActorOptions("name", dummy)));

    expectIllegalArgumentException(
        () -> new ReplicaConfig(deploymentDef, null, getRayActorOptions("max_restarts", dummy)));

    expectIllegalArgumentException(
        () -> new ReplicaConfig(deploymentDef, null, getRayActorOptions("num_cpus", -1.0)));
    ReplicaConfig replicaConfig =
        new ReplicaConfig(deploymentDef, null, getRayActorOptions("num_cpus", 2.0));
    Assert.assertEquals(replicaConfig.getResource().get("CPU").doubleValue(), 2.0);

    expectIllegalArgumentException(
        () -> new ReplicaConfig(deploymentDef, null, getRayActorOptions("num_gpus", -1.0)));
    replicaConfig = new ReplicaConfig(deploymentDef, null, getRayActorOptions("num_gpus", 2.0));
    Assert.assertEquals(replicaConfig.getResource().get("GPU").doubleValue(), 2.0);

    expectIllegalArgumentException(
        () -> new ReplicaConfig(deploymentDef, null, getRayActorOptions("memory", -1.0)));
    replicaConfig = new ReplicaConfig(deploymentDef, null, getRayActorOptions("memory", 2.0));
    Assert.assertEquals(replicaConfig.getResource().get("memory").doubleValue(), 2.0);

    expectIllegalArgumentException(
        () ->
            new ReplicaConfig(
                deploymentDef, null, getRayActorOptions("object_store_memory", -1.0)));
    replicaConfig =
        new ReplicaConfig(deploymentDef, null, getRayActorOptions("object_store_memory", 2.0));
    Assert.assertEquals(replicaConfig.getResource().get("object_store_memory").doubleValue(), 2.0);
  }

  private void expectIllegalArgumentException(Validator validator) {
    try {
      validator.validate();
      Assert.assertTrue(false, "expect IllegalArgumentException");
    } catch (IllegalArgumentException e) {

    }
  }

  private Map<String, Object> getRayActorOptions(String key, Object value) {
    Map<String, Object> rayActorOptions = new HashMap<>();
    rayActorOptions.put(key, value);
    return rayActorOptions;
  }
}
