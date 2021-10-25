package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.runtime.config.RayConfig;
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
  public void testIsolationBetweenDiffrentNamespaces() throws IOException, InterruptedException {
    System.setProperty("ray.job.namespace", "test2");
    Process driver = null;
    try {
      Ray.init();
      driver = startDriverWithNamespace();
      // Wait for driver to start.
      TimeUnit.SECONDS.sleep(3);
      Assert.assertThrows(
          NoSuchElementException.class,
          () -> {
            Ray.getGlobalActor("a").get();
          });
    } finally {
      if (driver != null) {
        driver.waitFor(1, TimeUnit.SECONDS);
      }
      Ray.shutdown();
    }
  }

  /// This case tests that actor can be accessed between different jobs but in the same namespace.
  public void testIsolationInTheSameNamespaces() throws IOException, InterruptedException {
    System.setProperty("ray.job.namespace", "test1");
    Process driver = null;
    try {
      Ray.init();
      driver = startDriverWithNamespace();
      // Wait for driver to start.
      TimeUnit.SECONDS.sleep(3);
      ActorHandle<A> a = (ActorHandle<A>) Ray.getGlobalActor("a").get();
      Assert.assertEquals("hello", a.task(A::hello).remote().get());
    } finally {
      if (driver != null) {
        driver.waitFor(1, TimeUnit.SECONDS);
      }
      Ray.shutdown();
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    System.setProperty("ray.job.namespace", "test1");
    Ray.init();
    ActorHandle<A> a = Ray.actor(A::new).setGlobalName("a").remote();
    Assert.assertEquals("hello", a.task(A::hello).remote().get());
    /// Because we don't support long running job yet, so sleep to don't destroy
    /// it for a while. Otherwise the actor created in this job will be destroyed
    /// as well.
    TimeUnit.SECONDS.sleep(10);
    Ray.shutdown();
  }

  private Process startDriverWithNamespace() throws IOException {
    RayConfig rayConfig = TestUtils.getRuntime().getRayConfig();

    ProcessBuilder builder =
        new ProcessBuilder(
            "java",
            "-cp",
            System.getProperty("java.class.path"),
            "-Dray.address=" + rayConfig.getRedisAddress(),
            "-Dray.object-store.socket-name=" + rayConfig.objectStoreSocketName,
            "-Dray.raylet.socket-name=" + rayConfig.rayletSocketName,
            "-Dray.raylet.node-manager-port=" + String.valueOf(rayConfig.getNodeManagerPort()),
            NamespaceTest.class.getName());
    builder.redirectError(ProcessBuilder.Redirect.INHERIT);
    return builder.start();
  }
}
