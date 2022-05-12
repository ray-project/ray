package io.ray.test;

import com.google.common.collect.ImmutableList;
import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.runtimeenv.RuntimeEnv;
import io.ray.runtime.util.SystemUtil;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "cluster")
public class RuntimeEnvTest {

  private static class A {

    public String getEnv(String key) {
      return System.getenv(key);
    }

    public int getPid() {
      return SystemUtil.pid();
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
    System.setProperty("ray.job.num-java-workers-per-process", "1");
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
    /// This is used to test that actors with runtime envs will not reuse worker process.
    System.setProperty("ray.job.num-java-workers-per-process", "2");
    try {
      Ray.init();
      int pid1 = 0;
      int pid2 = 0;
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

        pid1 = actor1.task(A::getPid).remote().get();
      }

      {
        /// Because we didn't set them for actor2 , all should be null.
        ActorHandle<A> actor2 = Ray.actor(A::new).remote();
        String val = actor2.task(A::getEnv, "KEY1").remote().get();
        Assert.assertNull(val);
        val = actor2.task(A::getEnv, "KEY2").remote().get();
        Assert.assertNull(val);
        pid2 = actor2.task(A::getPid).remote().get();
      }

      // actor1 and actor2 shouldn't be in one process because they have
      // different runtime env.
      Assert.assertNotEquals(pid1, pid2);
    } finally {
      Ray.shutdown();
    }
  }

  public void testPerActorEnvVarsOverwritePerJobEnvVars() {
    System.setProperty("ray.job.num-java-workers-per-process", "2");
    System.setProperty("ray.job.runtime-env.env-vars.KEY1", "A");
    System.setProperty("ray.job.runtime-env.env-vars.KEY2", "B");

    int pid1 = 0;
    int pid2 = 0;
    try {
      Ray.init();
      {
        RuntimeEnv runtimeEnv = new RuntimeEnv.Builder().addEnvVar("KEY1", "C").build();

        ActorHandle<A> actor1 = Ray.actor(A::new).setRuntimeEnv(runtimeEnv).remote();
        String val = actor1.task(A::getEnv, "KEY1").remote().get();
        Assert.assertEquals(val, "C");
        val = actor1.task(A::getEnv, "KEY2").remote().get();
        Assert.assertEquals(val, "B");
        pid1 = actor1.task(A::getPid).remote().get();
      }

      {
        /// Because we didn't set them for actor2 explicitly, it should use the per job
        /// runtime env.
        ActorHandle<A> actor2 = Ray.actor(A::new).remote();
        String val = actor2.task(A::getEnv, "KEY1").remote().get();
        Assert.assertEquals(val, "A");
        val = actor2.task(A::getEnv, "KEY2").remote().get();
        Assert.assertEquals(val, "B");
        pid2 = actor2.task(A::getPid).remote().get();
      }

      // actor1 and actor2 shouldn't be in one process because they have
      // different runtime env.
      Assert.assertNotEquals(pid1, pid2);
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
    testDownloadAndLoadPackagesForTask(
        "https://github.com/ray-project/test_packages/raw/main/raw_resources/bar.jar",
        "io.testpackages.Bar");
  }

  public void testZipPackageForTask() {
    testDownloadAndLoadPackagesForTask(
        "https://github.com/ray-project/test_packages/raw/main/raw_resources/foo.zip",
        "io.testpackages.Foo");
  }

  /// This case tests that a task needs 2 jars for load different classes.
  public void testMultipleJars() {
    testDownloadAndLoadPackagesForTask(
        ImmutableList.of(
            "https://github.com/ray-project/test_packages/raw/main/raw_resources/bar.jar",
            "https://github.com/ray-project/test_packages/raw/main/raw_resources/foo.jar"),
        ImmutableList.of("io.testpackages.Bar", "io.testpackages.Foo"));
  }
}
