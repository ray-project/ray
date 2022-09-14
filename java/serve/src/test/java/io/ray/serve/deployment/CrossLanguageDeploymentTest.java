package io.ray.serve.deployment;

import io.ray.api.Ray;
import io.ray.serve.BaseServeTest;
import io.ray.serve.api.Serve;
import io.ray.serve.generated.DeploymentLanguage;
import io.ray.serve.handle.RayServeHandle;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class CrossLanguageDeploymentTest extends BaseServeTest {
  private static final String PYTHON_MODULE = "test_python_deployment";

  @BeforeClass
  public void beforeClass() {
    // Delete and re-create the temp dir.
    File tempDir =
        new File(
            System.getProperty("java.io.tmpdir")
                + File.separator
                + "ray_serve_cross_language_test");
    FileUtils.deleteQuietly(tempDir);
    tempDir.mkdirs();
    tempDir.deleteOnExit();

    // Write the test Python file to the temp dir.
    InputStream in =
        CrossLanguageDeploymentTest.class.getResourceAsStream(
            File.separator + PYTHON_MODULE + ".py");
    File pythonFile = new File(tempDir.getAbsolutePath() + File.separator + PYTHON_MODULE + ".py");
    try {
      FileUtils.copyInputStreamToFile(in, pythonFile);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    System.setProperty(
        "ray.job.code-search-path",
        System.getProperty("java.class.path") + File.pathSeparator + tempDir.getAbsolutePath());
  }

  @Test
  public void createPyClassTest() {
    Deployment deployment =
        Serve.deployment()
            .setLanguage(DeploymentLanguage.PYTHON)
            .setName("createPyClassTest")
            .setDeploymentDef(PYTHON_MODULE + ".Counter")
            .setNumReplicas(1)
            .setInitArgs(new Object[] {"28"})
            .create();

    deployment.deploy(true);
    Assert.assertEquals(Ray.get(deployment.getHandle().method("increase").remote("6")), "34");
  }

  @Test
  public void createPyMethodTest() {
    Deployment deployment =
        Serve.deployment()
            .setLanguage(DeploymentLanguage.PYTHON)
            .setName("createPyMethodTest")
            .setDeploymentDef(PYTHON_MODULE + ".echo_server")
            .setNumReplicas(1)
            .create();
    deployment.deploy(true);
    RayServeHandle handle = deployment.getHandle();
    Assert.assertEquals(Ray.get(handle.method("__call__").remote("6")), "6");
  }

  @Test
  public void userConfigTest() throws InterruptedException {
    Deployment deployment =
        Serve.deployment()
            .setLanguage(DeploymentLanguage.PYTHON)
            .setName("userConfigTest")
            .setDeploymentDef(PYTHON_MODULE + ".Counter")
            .setNumReplicas(1)
            .setUserConfig("1")
            .setInitArgs(new Object[] {"28"})
            .create();
    deployment.deploy(true);
    Assert.assertEquals(Ray.get(deployment.getHandle().method("increase").remote("6")), "7");
    deployment.options().setUserConfig("3").create().deploy(true);
    TimeUnit.SECONDS.sleep(20L);
    Assert.assertEquals(Ray.get(deployment.getHandle().method("increase").remote("6")), "9");
  }
}
