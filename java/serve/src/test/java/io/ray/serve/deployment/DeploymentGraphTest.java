package io.ray.serve.deployment;

import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.serve.api.Serve;
import io.ray.serve.config.RayServeConfig;
import io.ray.serve.handle.RayServeHandle;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DeploymentGraphTest {

  private String previousHttpPort;

  @BeforeMethod(alwaysRun = true)
  public void before() {
    // The default port 8000 is occupied by other processes on the ci platform.
    previousHttpPort = System.getProperty(RayServeConfig.PROXY_HTTP_PORT);
    System.setProperty(
        RayServeConfig.PROXY_HTTP_PORT, "8341"); // TODO(liuyang-my) Get an available port.
  }

  @AfterMethod(alwaysRun = true)
  public void after() {
    if (previousHttpPort == null) {
      System.clearProperty(RayServeConfig.PROXY_HTTP_PORT);
    } else {
      System.setProperty(RayServeConfig.PROXY_HTTP_PORT, previousHttpPort);
    }
  }

  public static class Counter {
    private AtomicInteger count;

    public Counter(String count) {
      this.count = new AtomicInteger(Integer.parseInt(count));
    }

    public String call(String input) {
      return String.valueOf(count.addAndGet(Integer.parseInt(input)));
    }
  }

  @Test
  public void bindTest() {
    Application deployment =
        Serve.deployment().setDeploymentDef(Counter.class.getName()).setNumReplicas(1).bind("2");

    RayServeHandle handle = Serve.run(deployment).get();
    Assert.assertEquals(Ray.get(handle.remote("2")), "4");
  }

  public static class ModelA {
    public String call(String msg) {
      return "A:" + msg;
    }
  }

  public static class ModelB {
    public String call(String msg) {
      return "B:" + msg;
    }
  }

  public static class Combiner {
    private RayServeHandle modelAHandle;
    private RayServeHandle modelBHandle;

    public Combiner(RayServeHandle modelAHandle, RayServeHandle modelBHandle) {
      this.modelAHandle = modelAHandle;
      this.modelBHandle = modelBHandle;
    }

    public String call(String request) {
      ObjectRef<Object> refA = modelAHandle.remote(request);
      ObjectRef<Object> refB = modelBHandle.remote(request);
      return refA.get() + "," + refB.get();
    }
  }

  @Test
  public void testPassHandle() {
    Application modelA = Serve.deployment().setDeploymentDef(ModelA.class.getName()).bind();
    Application modelB = Serve.deployment().setDeploymentDef(ModelB.class.getName()).bind();

    Application driver =
        Serve.deployment().setDeploymentDef(Combiner.class.getName()).bind(modelA, modelB);
    RayServeHandle handle = Serve.run(driver).get();
    Assert.assertEquals(handle.remote("test"), "A:test,B:test");
  }
}
