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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CrossLanguageInvocationTest extends BaseMultiLanguageTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(CrossLanguageInvocationTest.class);
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

  @Test
  public void testPythonCallJavaFunction() {
    RayObject res = Ray.callPy(PYTHON_MODULE, "py_func_call_java_function", "hello".getBytes());
    Assert.assertEquals(res.get(), "[Python]py_func -> [Java]bytesEcho -> hello".getBytes());
  }

  @Test(groups = {"directCall"})
  public void testCallingPythonActor() {
    // Python worker doesn't support direct call yet.
    TestUtils.skipTestIfDirectActorCallEnabled();
    RayPyActor actor = Ray.createPyActor(PYTHON_MODULE, "Counter", "1".getBytes());
    RayObject res = Ray.callPy(actor, "increase", "1".getBytes());
    Assert.assertEquals(res.get(), "2".getBytes());
  }

  @Test
  public void testPythonCallJavaActor() {
    RayObject res = Ray.callPy(PYTHON_MODULE, "py_func_call_java_actor", "1".getBytes());
    Assert.assertEquals(res.get(), "Counter1".getBytes());
  }

  public static byte[] bytesEcho(byte[] value) {
    // This function will be called from test_cross_language_invocation.py
    String valueStr = new String(value);
    LOGGER.debug(String.format("bytesEcho called with: %s", valueStr));
    return ("[Java]bytesEcho -> " + valueStr).getBytes();
  }

  public static class TestActor {
    public TestActor(byte[] v) {
      value = v;
    }

    public byte[] concat(byte[] v) {
      byte[] c = new byte[value.length + v.length];
      System.arraycopy(value, 0, c, 0, value.length);
      System.arraycopy(v, 0, c, value.length, v.length);
      return c;
    }

    private byte[] value;
  }
}
