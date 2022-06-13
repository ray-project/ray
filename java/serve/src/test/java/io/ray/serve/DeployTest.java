package io.ray.serve;

import io.ray.api.Ray;
import io.ray.serve.api.Serve;
import io.ray.serve.deployment.Deployment;
import io.ray.serve.deployment.DeploymentRoute;
import io.ray.serve.util.ExampleEchoDeployment;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DeployTest extends BaseServeTest {

  public static class Counter {

    private AtomicInteger count;

    public Counter(Integer value) {
      this.count = new AtomicInteger(value);
    }

    public Integer call(Integer delta) {
      return this.count.addAndGet(delta);
    }
  }

  @Test(enabled = false)
  public static void testDepoly() throws IOException {
    // Deploy deployment.
    String deploymentName = "counter";

    Deployment deployment =
        Serve.deployment()
            .setName(deploymentName)
            .setDeploymentDef(Counter.class.getName())
            .setNumReplicas(2)
            .setInitArgs(new Object[] {10})
            .create();

    deployment.deploy(true);

    Deployment result = Serve.getDeployment(deploymentName);
    DeploymentRoute deploymentInfo = client.getDeploymentInfo(deploymentName);

    // Call deployment by handle.
    Assert.assertEquals(16, Ray.get(deployment.getHandle().method("f").remote(6)));
    // TODO Assert.assertEquals(16, Ray.get(deployment.getHandle().method("f",
    // "signature").remote(6)));
    Assert.assertEquals(26, Ray.get(client.getHandle(deploymentName, false).remote(10)));
  }

  @Test
  public static void testDepoly2() throws InterruptedException {
    // Deploy deployment.
    String deploymentName = "exampleEcho";

    Deployment deployment =
        Serve.deployment()
            .setName(deploymentName)
            .setDeploymentDef(ExampleEchoDeployment.class.getName())
            .setNumReplicas(1)
            .setInitArgs(new Object[] {"echo_"})
            .create();

    deployment.deploy(true);
    Assert.assertEquals("echo_6", Ray.get(deployment.getHandle().method("call").remote(6)));
    TimeUnit.MINUTES.sleep(10);
    Assert.assertTrue((boolean) Ray.get(deployment.getHandle().method("checkHealth").remote()));
  }
}
