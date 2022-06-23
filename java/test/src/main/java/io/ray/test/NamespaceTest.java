package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.options.ActorLifetime;
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
                  Ray.getActor("a").get();
                }));
  }

  /// This case tests that actor can be accessed between different jobs but in the same namespace.
  public void testIsolationInTheSameNamespaces() throws IOException, InterruptedException {
    System.setProperty("ray.job.namespace", "test1");
    testIsolation(
        MainClassForNamespaceTest.class,
        () -> {
          ActorHandle<A> a = (ActorHandle<A>) Ray.getActor("a").get();
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
                  Ray.getActor("a").get();
                }));
  }

  private static String getNamespace() {
    return Ray.getRuntimeContext().getNamespace();
  }

  private static class GetNamespaceActor {
    public String getNamespace() {
      return Ray.getRuntimeContext().getNamespace();
    }
  }

  public void testGetNamespace() {
    final String thisNamespace = "test_get_current_namespace";
    System.setProperty("ray.job.namespace", thisNamespace);
    try {
      Ray.init();
      /// Test in driver.
      Assert.assertEquals(thisNamespace, Ray.getRuntimeContext().getNamespace());
      /// Test in task.
      Assert.assertEquals(thisNamespace, Ray.task(NamespaceTest::getNamespace).remote().get());
      /// Test in actor.
      ActorHandle<GetNamespaceActor> a = Ray.actor(GetNamespaceActor::new).remote();
      Assert.assertEquals(thisNamespace, a.task(GetNamespaceActor::getNamespace).remote().get());
    } finally {
      Ray.shutdown();
    }
  }

  public static class MainClassForNamespaceTest {
    public static void main(String[] args) throws IOException, InterruptedException {
      System.setProperty("ray.job.namespace", "test1");
      startDriver();
    }
  }

  public static class MainClassForAnonymousNamespaceTest {
    public static void main(String[] args) throws IOException, InterruptedException {
      startDriver();
    }
  }

  private static void startDriver() throws InterruptedException {
    Ray.init();
    ActorHandle<A> a = Ray.actor(A::new).setLifetime(ActorLifetime.DETACHED).setName("a").remote();
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
      driver.waitFor(10, TimeUnit.SECONDS);
      runnable.run();
    } finally {
      Ray.shutdown();
    }
  }
}
