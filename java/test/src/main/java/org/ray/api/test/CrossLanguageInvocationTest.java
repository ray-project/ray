package org.ray.api.test;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.RayPyActor;
import org.ray.api.TestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CrossLanguageInvocationTest extends BaseMultiLanguageTest {

  private static final String PYTHON_MODULE = "test_cross_language_invocation";

  @Override
  protected Map<String, String> getRayStartEnv() {
    // Delete and re-create the temp dir.
    File tempDir = new File(
        System.getProperty("java.io.tmpdir") + File.separator + "ray_cross_language_test");
    FileUtils.deleteQuietly(tempDir);
    tempDir.mkdirs();
    tempDir.deleteOnExit();

    // Write the test Python file to the temp dir.
    InputStream in = CrossLanguageInvocationTest.class
        .getResourceAsStream("/" + PYTHON_MODULE + ".py");
    File pythonFile = new File(
        tempDir.getAbsolutePath() + File.separator + PYTHON_MODULE + ".py");
    try {
      FileUtils.copyInputStreamToFile(in, pythonFile);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return ImmutableMap.of("PYTHONPATH", tempDir.getAbsolutePath());
  }

  @Test
  public void testCallingPythonFunction() {
    RayObject res = Ray.callPy(PYTHON_MODULE, "py_func", "hello".getBytes());
    Assert.assertEquals(res.get(), "Response from Python: hello".getBytes());
  }

  @Test(groups = {"directCall"})
  public void testCallingPythonActor() {
    // Python worker doesn't support direct call yet.
    TestUtils.skipTestIfDirectActorCallEnabled();
    RayPyActor actor = Ray.createPyActor(PYTHON_MODULE, "Counter", "1".getBytes());
    RayObject res = Ray.callPy(actor, "increase", "1".getBytes());
    Assert.assertEquals(res.get(), "2".getBytes());
  }
}
