package io.ray.test;

import com.google.common.collect.ImmutableList;
import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.runtimeenv.RuntimeEnv;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "cluster")
public class RuntimeEnvTest {

  private static final String FOO_JAR_URL =
      "https://github.com/ray-project/test_packages/raw/main/raw_resources/foo.jar";
  private static final String BAR_JAR_URL =
      "https://github.com/ray-project/test_packages/raw/main/raw_resources/bar.jar";
  private static final String FOO_ZIP_URL =
      "https://github.com/ray-project/test_packages/raw/main/raw_resources/foo.zip";
  private static final String BAR_ZIP_URL =
      "https://github.com/ray-project/test_packages/raw/main/raw_resources/bar.zip";

  private static final String FOO_CLASS_NAME = "io.testpackages.Foo";
  private static final String BAR_CLASS_NAME = "io.testpackages.Bar";

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

  public void testPerActorEnvVars() {
    try {
      Ray.init();
      {
        RuntimeEnv runtimeEnv =
            new RuntimeEnv.Builder()
                .addEnvVar("KEY1", "A")
                .addEnvVar("KEY2", "B")
                .addEnvVar("KEY1", "C")
                .build();

        ActorHandle<A> actor1 = Ray.actor(A::new).setRuntimeEnv(runtimeEnv).remote();
        String val = actor1.task(A::getEnv, "KEY1").remote().get();
        Assert.assertEquals(val, "C");
        val = actor1.task(A::getEnv, "KEY2").remote().get();
        Assert.assertEquals(val, "B");
      }

      {
        /// Because we didn't set them for actor2 , all should be null.
        ActorHandle<A> actor2 = Ray.actor(A::new).remote();
        String val = actor2.task(A::getEnv, "KEY1").remote().get();
        Assert.assertNull(val);
        val = actor2.task(A::getEnv, "KEY2").remote().get();
        Assert.assertNull(val);
      }

    } finally {
      Ray.shutdown();
    }
  }

  public void testPerActorEnvVarsOverwritePerJobEnvVars() {
    System.setProperty("ray.job.runtime-env.env-vars.KEY1", "A");
    System.setProperty("ray.job.runtime-env.env-vars.KEY2", "B");

    try {
      Ray.init();
      {
        RuntimeEnv runtimeEnv = new RuntimeEnv.Builder().addEnvVar("KEY1", "C").build();

        ActorHandle<A> actor1 = Ray.actor(A::new).setRuntimeEnv(runtimeEnv).remote();
        String val = actor1.task(A::getEnv, "KEY1").remote().get();
        Assert.assertEquals(val, "C");
        val = actor1.task(A::getEnv, "KEY2").remote().get();
        Assert.assertNull(val);
      }

      {
        /// Because we didn't set them for actor2 explicitly, it should use the per job
        /// runtime env.
        ActorHandle<A> actor2 = Ray.actor(A::new).remote();
        String val = actor2.task(A::getEnv, "KEY1").remote().get();
        Assert.assertEquals(val, "A");
        val = actor2.task(A::getEnv, "KEY2").remote().get();
        Assert.assertEquals(val, "B");
      }

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
      Assert.assertNull(val);
    } finally {
      Ray.shutdown();
    }
  }

  private static void testDownloadAndLoadPackage(String url) {
    try {
      Ray.init();
      final RuntimeEnv runtimeEnv = new RuntimeEnv.Builder().addJars(ImmutableList.of(url)).build();
      ActorHandle<A> actor1 = Ray.actor(A::new).setRuntimeEnv(runtimeEnv).remote();
      boolean ret = actor1.task(A::findClass, FOO_CLASS_NAME).remote().get();
      Assert.assertTrue(ret);
    } finally {
      Ray.shutdown();
    }
  }

  public void testJarPackageInActor() {
    testDownloadAndLoadPackage(FOO_JAR_URL);
  }

  public void testZipPackageInActor() {
    testDownloadAndLoadPackage(FOO_ZIP_URL);
  }

  private static boolean findClasses(List<String> classNames) {
    try {
      for (String name : classNames) {
        Class.forName(name);
      }
    } catch (ClassNotFoundException e) {
      return false;
    }
    return true;
  }

  private static void testDownloadAndLoadPackagesForTask(
      List<String> urls, List<String> classNames) {
    try {
      Ray.init();
      final RuntimeEnv runtimeEnv = new RuntimeEnv.Builder().addJars(urls).build();
      boolean ret =
          Ray.task(RuntimeEnvTest::findClasses, classNames)
              .setRuntimeEnv(runtimeEnv)
              .remote()
              .get();
      Assert.assertTrue(ret);
    } finally {
      Ray.shutdown();
    }
  }

  private static void testDownloadAndLoadPackagesForTask(String url, String className) {
    testDownloadAndLoadPackagesForTask(ImmutableList.of(url), ImmutableList.of(className));
  }

  public void testJarPackageForTask() {
    testDownloadAndLoadPackagesForTask(BAR_JAR_URL, BAR_CLASS_NAME);
  }

  public void testZipPackageForTask() {
    testDownloadAndLoadPackagesForTask(FOO_ZIP_URL, FOO_CLASS_NAME);
  }

  /// This case tests that a task needs 2 jars for load different classes.
  public void testMultipleJars() {
    testDownloadAndLoadPackagesForTask(
        ImmutableList.of(FOO_JAR_URL, BAR_JAR_URL),
        ImmutableList.of(BAR_CLASS_NAME, FOO_CLASS_NAME));
  }

  public void testRuntimeEnvJarsForJob() {
    System.setProperty("ray.job.runtime-env.jars.0", FOO_JAR_URL);
    System.setProperty("ray.job.runtime-env.jars.1", BAR_JAR_URL);
    try {
      Ray.init();
      boolean ret =
          Ray.task(RuntimeEnvTest::findClasses, ImmutableList.of(BAR_CLASS_NAME, FOO_CLASS_NAME))
              .remote()
              .get();
      Assert.assertTrue(ret);
    } finally {
      Ray.shutdown();
    }
  }
}
