package org.ray.api.test;

import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.RayPyActor;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CrossLanguageInvocationTest extends BaseMultiLanguageTest {

  @Test
  public void testCallingPythonFunction() {
    RayObject res = Ray.callPy("test_cross_language_invocation",
        "py_func", "hello".getBytes());
    Assert.assertEquals(res.get(), "Response from Python: hello".getBytes());
  }

  @Test
  public void testCallingPythonActor() {
    RayPyActor actor = Ray
        .createPyActor("test_cross_language_invocation", "Counter", "1".getBytes());
    RayObject res = Ray.callPy(actor, "increase", "1".getBytes());
    Assert.assertEquals(res.get(), "2".getBytes());
  }
}
