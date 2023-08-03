package io.ray.serve.replica;

import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ReplicaNameTest {

  @Test
  public void test() {
    String deploymentTag = "ReplicaNameTest";
    String replicaSuffix = RandomStringUtils.randomAlphabetic(6);
    ReplicaName replicaName = new ReplicaName(deploymentTag, replicaSuffix);

    Assert.assertEquals(replicaName.getDeploymentTag(), deploymentTag);
    Assert.assertEquals(replicaName.getReplicaSuffix(), replicaSuffix);
    Assert.assertEquals(replicaName.getReplicaTag(), deploymentTag + "#" + replicaSuffix);
  }
}
