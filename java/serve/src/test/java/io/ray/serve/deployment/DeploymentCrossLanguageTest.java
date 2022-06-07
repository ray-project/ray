package io.ray.serve.deployment;

import io.ray.api.ObjectRef;
import io.ray.api.exception.CrossLanguageException;
import io.ray.serve.api.Serve;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DeploymentCrossLanguageTest {

  private static final String PYTHON_MODULE = "test_deployment_cross_language_invocation";

  private String suffix = "_" + System.currentTimeMillis();

  @BeforeClass
  public void beforeClass() {
    // Delete and re-create the temp dir.
    File tempDir =
        new File(System.getProperty("java.io.tmpdir") + File.separator + "ray_cross_language_test");
    FileUtils.deleteQuietly(tempDir);
    tempDir.mkdirs();
    tempDir.deleteOnExit();

    // Write the test Python file to the temp dir.
    InputStream in =
        DeploymentCrossLanguageTest.class.getResourceAsStream("/" + PYTHON_MODULE + ".py");
    File pythonFile = new File(tempDir.getAbsolutePath() + File.separator + PYTHON_MODULE + ".py");
    try {
      FileUtils.copyInputStreamToFile(in, pythonFile);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    System.setProperty(
        "ray.job.code-search-path",
        System.getProperty("java.class.path") + File.pathSeparator + tempDir.getAbsolutePath());

    // Start serve.
    Serve.start(false, true, null, null, null);
  }

  @AfterClass(alwaysRun = false)
  public void afterClass() {
    Serve.shutdown();
  }

  @Test
  public void testCallingPythonFunction() {
    Deployment deployment =
        Serve.deployment()
            .setName("testCallingPythonFunction" + suffix)
            .setDeploymentDef(PYTHON_MODULE + "." + "py_return_input")
            .setNumReplicas(1)
            .create();
    deployment.deploy(true);

    Object[] inputs =
        new Object[] {
          true, // Boolean
          Byte.MAX_VALUE, // Byte
          Short.MAX_VALUE, // Short
          Integer.MAX_VALUE, // Integer
          Long.MAX_VALUE, // Long
          // BigInteger can support max value of 2^64-1, please refer to:
          // https://github.com/msgpack/msgpack/blob/master/spec.md#int-format-family
          // If BigInteger larger than 2^64-1, the value can only be transferred among Java workers.
          BigInteger.valueOf(Long.MAX_VALUE), // BigInteger
          "Hello World!", // String
          1.234f, // Float
          1.234, // Double
          "example binary".getBytes()
        }; // byte[]

    for (Object o : inputs) {
      ObjectRef res = deployment.getHandle().remote(o);
      Assert.assertEquals(res.get(), o);
    }
  }

  @Test
  public void testCallingPythonActor() {
    Deployment deployment =
        Serve.deployment()
            .setName("")
            .setDeploymentDef(PYTHON_MODULE + "." + "Counter")
            .setNumReplicas(1)
            .create();

    ObjectRef<Object> res = deployment.getHandle().method("increase").remote("1");
     Assert.assertEquals(res.get(), "2".getBytes());
  }

  @Test
  public void testRaiseExceptionFromPython() {
    Deployment deployment =
        Serve.deployment()
            .setName("")
            .setDeploymentDef(PYTHON_MODULE + "." + "py_func_python_raise_exception")
            .setNumReplicas(1)
            .create();

    ObjectRef<Object> res = deployment.getHandle().remote();
    try {
      res.get();
    } catch (RuntimeException ex) {
      // ex is a Python exception(py_func_python_raise_exception) with no cause.
      Assert.assertTrue(ex instanceof CrossLanguageException);
      CrossLanguageException e = (CrossLanguageException) ex;
      // ex.cause is null.
      Assert.assertNull(ex.getCause());
      Assert.assertTrue(
          ex.getMessage().contains("ZeroDivisionError: division by zero"), ex.getMessage());
      return;
    }
    Assert.fail();
  }
}
