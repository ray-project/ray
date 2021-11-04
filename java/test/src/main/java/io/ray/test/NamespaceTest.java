package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "cluster")
public class NamespaceTest {

  private static class A {
    public String hello() {
      return "hello";
    }
  }

  /// This case tests that actor cannot be accessed in different namespaces.
  public void testIsolationBetweenNamespaces() throws IOException, InterruptedException {
    System.setProperty("ray.job.namespace", "test2");
    testIsolation(
        MainClassForNamespaceTest.class,
        () ->
            Assert.assertThrows(
                NoSuchElementException.class,
                () -> {
                  Ray.getGlobalActor("a").get();
                }));
  }

  /// This case tests that actor can be accessed between different jobs but in the same namespace.
  public void testIsolationInTheSameNamespaces() throws IOException, InterruptedException {
    System.setProperty("ray.job.namespace", "test1");
    testIsolation(
        MainClassForNamespaceTest.class,
        () -> {
          ActorHandle<A> a = (ActorHandle<A>) Ray.getGlobalActor("a").get();
          Assert.assertEquals("hello", a.task(A::hello).remote().get());
        });
  }

  public void testIsolationBetweenAnonymousNamespaces() throws IOException, InterruptedException {
    NamespaceTest.testIsolation(
        MainClassForAnonymousNamespaceTest.class,
        () ->
            Assert.assertThrows(
                NoSuchElementException.class,
                () -> {
                  Ray.getGlobalActor("a").get();
                }));
  }

  public static class MainClassForNamespaceTest {
    public static void main(String[] args) throws IOException, InterruptedException {
      System.setProperty("ray.job.namespace", "test1");
      startDriverWithGlobalActor();
    }
  }

  public static class MainClassForAnonymousNamespaceTest {
    public static void main(String[] args) throws IOException, InterruptedException {
      startDriverWithGlobalActor();
    }
  }

  private static void startDriverWithGlobalActor() throws InterruptedException {
    Ray.init();
    ActorHandle<A> a = Ray.actor(A::new).setGlobalName("a").remote();
    Assert.assertEquals("hello", a.task(A::hello).remote().get());
    /// Because we don't support long running job yet, so sleep to don't destroy
    /// it for a while. Otherwise the actor created in this job will be destroyed
    /// as well.
    TimeUnit.SECONDS.sleep(10);
    Ray.shutdown();
  }

  private static void testIsolation(Class<?> driverClass, Runnable runnable)
      throws IOException, InterruptedException {
    Process driver = null;
    try {
      Ray.init();
      ProcessBuilder builder = TestUtils.buildDriver(driverClass, null);
      builder.redirectError(ProcessBuilder.Redirect.INHERIT);
      driver = builder.start();
      // Wait for driver to start.
      TimeUnit.SECONDS.sleep(3);
      runnable.run();
    } finally {
      if (driver != null) {
        driver.waitFor(1, TimeUnit.SECONDS);
      }
      Ray.shutdown();
    }
  }
}
