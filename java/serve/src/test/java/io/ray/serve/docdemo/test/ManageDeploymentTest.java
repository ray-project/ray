package io.ray.serve.docdemo.test;

import io.ray.serve.BaseServeTest;
import io.ray.serve.deployment.Deployment;
import io.ray.serve.docdemo.ManageDeployment;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ManageDeploymentTest extends BaseServeTest {

  @Test(groups = {"cluster"})
  public void test() {
    ManageDeployment manageDeployment = new ManageDeployment();
    manageDeployment.create();

    Deployment deployment = manageDeployment.query();
    Assert.assertEquals(deployment.getName(), "counter");
    Assert.assertEquals(deployment.getConfig().getNumReplicas().intValue(), 1);
    Assert.assertEquals(deployment.getInitArgs()[0], "1");

    manageDeployment.update();
    deployment = manageDeployment.query();
    Assert.assertEquals(deployment.getInitArgs()[0], "2");

    manageDeployment.scaleOut();
    deployment = manageDeployment.query();
    Assert.assertEquals(deployment.getConfig().getNumReplicas().intValue(), 1);
  }
}
