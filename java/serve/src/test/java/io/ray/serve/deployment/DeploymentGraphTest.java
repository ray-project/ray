package io.ray.serve.deployment;

import io.ray.api.Ray;
import io.ray.serve.BaseServeTest2;
import io.ray.serve.api.Serve;
import io.ray.serve.common.Constants;
import io.ray.serve.config.RayServeConfig;
import io.ray.serve.generated.ApplicationStatus;
import io.ray.serve.generated.StatusOverview;
import io.ray.serve.handle.DeploymentHandle;
import io.ray.serve.handle.DeploymentResponse;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DeploymentGraphTest extends BaseServeTest2 {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeploymentGraphTest.class);

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
    try {
      Serve.shutdown();
    } catch (Exception e) {
      LOGGER.error("serve shutdown error", e);
    }
    try {
      Ray.shutdown();
      LOGGER.info("Base serve test shutdown ray. Is initialized:{}", Ray.isInitialized());
    } catch (Exception e) {
      LOGGER.error("ray shutdown error", e);
    }
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

  @Test(groups = {"cluster"})
  public void bindTest() {
    Application deployment =
        Serve.deployment().setDeploymentDef(Counter.class.getName()).setNumReplicas(1).bind("2");

    DeploymentHandle handle = Serve.run(deployment);
    Assert.assertEquals(handle.remote("2").result(), "4");
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
    private DeploymentHandle modelAHandle;
    private DeploymentHandle modelBHandle;

    public Combiner(DeploymentHandle modelAHandle, DeploymentHandle modelBHandle) {
      LOGGER.info("Combiner.");
      this.modelAHandle = modelAHandle;
      this.modelBHandle = modelBHandle;
    }

    public String call(String request) {
      DeploymentResponse responseA = modelAHandle.remote(request);
      DeploymentResponse responseB = modelBHandle.remote(request);
      return responseA.result() + "," + responseB.result();
    }
  }

  @Test(groups = {"cluster"})
  public void testPassHandle() {
    Application modelA = Serve.deployment().setDeploymentDef(ModelA.class.getName()).bind();
    Application modelB = Serve.deployment().setDeploymentDef(ModelB.class.getName()).bind();

    Application driver =
        Serve.deployment().setDeploymentDef(Combiner.class.getName()).bind(modelA, modelB);
    DeploymentHandle handle = Serve.run(driver);
    Assert.assertEquals(handle.remote("test").result(), "A:test,B:test");
  }

  @Test(groups = {"cluster"})
  public void statusTest() {
    Application deployment =
        Serve.deployment().setDeploymentDef(Counter.class.getName()).setNumReplicas(1).bind("2");
    Serve.run(deployment);

    StatusOverview status =
        Serve.getGlobalClient().getServeStatus(Constants.SERVE_DEFAULT_APP_NAME);
    Assert.assertEquals(
        status.getAppStatus().getStatus(), ApplicationStatus.APPLICATION_STATUS_RUNNING);

    Serve.delete(Constants.SERVE_DEFAULT_APP_NAME);
    status = Serve.getGlobalClient().getServeStatus(Constants.SERVE_DEFAULT_APP_NAME);
    Assert.assertEquals(
        status.getAppStatus().getStatus(), ApplicationStatus.APPLICATION_STATUS_NOT_STARTED);
  }
}
