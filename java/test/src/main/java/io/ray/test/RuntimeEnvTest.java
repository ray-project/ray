package io.ray.test;

import com.google.common.collect.ImmutableList;
import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.runtimeenv.RuntimeEnv;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "cluster")
public class RuntimeEnvTest {

  private static class A {

    public String getEnv(String key) {
      return System.getenv(key);
    }

    public boolean findClass(String className) {
      try {
        Class.forName(className);
      } catch (ClassNotFoundException e) {
        return false;
      }
      return true;
    }
  }

  public void testPerJobEnvVars() {
    System.setProperty("ray.job.runtime-env.env-vars.KEY1", "A");
    System.setProperty("ray.job.runtime-env.env-vars.KEY2", "B");

    try {
      Ray.init();
      ActorHandle<A> actor = Ray.actor(A::new).remote();
      String val = actor.task(A::getEnv, "KEY1").remote().get();
      Assert.assertEquals(val, "A");
      val = actor.task(A::getEnv, "KEY2").remote().get();
      Assert.assertEquals(val, "B");
    } finally {
      Ray.shutdown();
    }
  }

  private static String getEnvVar(String key) {
    return System.getenv(key);
  }

  public void testEnvVarsForNormalTask() {
    try {
      Ray.init();
      RuntimeEnv runtimeEnv =
          new RuntimeEnv.Builder()
              .addEnvVar("KEY1", "A")
              .addEnvVar("KEY2", "B")
              .addEnvVar("KEY1", "C")
              .build();

      String val =
          Ray.task(RuntimeEnvTest::getEnvVar, "KEY1").setRuntimeEnv(runtimeEnv).remote().get();
      Assert.assertEquals(val, "C");
      val = Ray.task(RuntimeEnvTest::getEnvVar, "KEY2").setRuntimeEnv(runtimeEnv).remote().get();
      Assert.assertEquals(val, "B");
    } finally {
      Ray.shutdown();
    }
  }

  /// overwrite the runtime env from job config.
  public void testPerTaskEnvVarsOverwritePerJobEnvVars() {
    System.setProperty("ray.job.runtime-env.env-vars.KEY1", "A");
    System.setProperty("ray.job.runtime-env.env-vars.KEY2", "B");
    try {
      Ray.init();
      RuntimeEnv runtimeEnv = new RuntimeEnv.Builder().addEnvVar("KEY1", "C").build();

      /// value of KEY1 is overwritten to `C` and KEY2s is extended from job config.
      String val =
          Ray.task(RuntimeEnvTest::getEnvVar, "KEY1").setRuntimeEnv(runtimeEnv).remote().get();
      Assert.assertEquals(val, "C");
      val = Ray.task(RuntimeEnvTest::getEnvVar, "KEY2").setRuntimeEnv(runtimeEnv).remote().get();
      Assert.assertEquals(val, "B");
    } finally {
      Ray.shutdown();
    }
  }

  private static void testDownloadAndLoadPackage(String url) {
    try {
      Ray.init();
      final RuntimeEnv runtimeEnv = new RuntimeEnv.Builder().addJars(ImmutableList.of(url)).build();
      ActorHandle<A> actor1 = Ray.actor(A::new).setRuntimeEnv(runtimeEnv).remote();
      boolean ret = actor1.task(A::findClass, "io.testpackages.Foo").remote().get();
      Assert.assertTrue(ret);
    } finally {
      Ray.shutdown();
    }
  }

  public void testJarPackageInActor() {
    testDownloadAndLoadPackage(
        "https://github.com/ray-project/test_packages/raw/main/raw_resources/java-1.0-SNAPSHOT.jar");
  }

  public void testZipPackageInActor() {
    testDownloadAndLoadPackage(
        "https://github.com/ray-project/test_packages/raw/main/raw_resources/java-1.0-SNAPSHOT.zip");
  }
}
