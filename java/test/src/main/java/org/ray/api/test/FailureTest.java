package org.ray.api.test;

import org.junit.Assert;
import org.junit.Test;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.exception.RayException;

public class FailureTest extends BaseTest {

  private static final String EXCEPTION_MESSAGE = "Oops";

  public static int badFunc() {
    throw new RuntimeException(EXCEPTION_MESSAGE);
  }

  public static class BadActor {

    public BadActor(boolean failOnCreation) {
      if (failOnCreation) {
        throw new RuntimeException(EXCEPTION_MESSAGE);
      }
    }

    public int func() {
      throw new RuntimeException(EXCEPTION_MESSAGE);
    }
  }

  private static void assertTaskFail(RayObject<?> rayObject) {
    try {
      rayObject.get();
      Assert.fail("Task didn't fail.");
    } catch (RayException e) {
      e.printStackTrace();
      Throwable rootCause = e.getCause();
      while (rootCause.getCause() != null) {
        rootCause = rootCause.getCause();
      }
      Assert.assertTrue(rootCause instanceof RuntimeException);
      Assert.assertEquals(rootCause.getMessage(), EXCEPTION_MESSAGE);
    }
  }

  @Test
  public void testNormalTaskFailure() {
    assertTaskFail(Ray.call(FailureTest::badFunc));
  }

  @Test
  public void testActorCreationFailure() {
    RayActor<BadActor> actor = Ray.createActor(BadActor::new, true);
    assertTaskFail(Ray.call(BadActor::func, actor));
  }

  @Test
  public void testActorTaskFailure() {
    RayActor<BadActor> actor = Ray.createActor(BadActor::new, false);
    assertTaskFail(Ray.call(BadActor::func, actor));
  }
}

