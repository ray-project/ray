package io.ray.test;

import static io.ray.api.runtimeenv.types.RuntimeEnvName.JARS;

import com.google.common.collect.ImmutableList;
import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.runtimeenv.RuntimeEnv;
import io.ray.api.runtimeenv.RuntimeEnvConfig;
import io.ray.api.runtimeenv.types.RuntimeEnvName;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
      System.clearProperty("ray.job.runtime-env.env-vars.KEY1");
      System.clearProperty("ray.job.runtime-env.env-vars.KEY2");
      Ray.shutdown();
    }
  }

  private static String getEnvVar(String key) {
    return System.getenv(key);
  }

  public void testEnvVarsForNormalTask() {
    try {
      Ray.init();
      RuntimeEnv runtimeEnv = new RuntimeEnv.Builder().build();
      Map<String, String> envMap =
          new HashMap<String, String>() {
            {
              put("KEY1", "A");
              put("KEY2", "B");
              put("KEY1", "C");
            }
          };
      runtimeEnv.set(RuntimeEnvName.ENV_VARS, envMap);

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
      Map<String, String> envMap =
          new HashMap<String, String>() {
            {
              put("KEY1", "C");
            }
          };
      RuntimeEnv runtimeEnv = new RuntimeEnv.Builder().build();
      runtimeEnv.set(RuntimeEnvName.ENV_VARS, envMap);

      /// value of KEY1 is overwritten to `C` and KEY2s is extended from job config.
      String val =
          Ray.task(RuntimeEnvTest::getEnvVar, "KEY1").setRuntimeEnv(runtimeEnv).remote().get();
      Assert.assertEquals(val, "C");
      val = Ray.task(RuntimeEnvTest::getEnvVar, "KEY2").setRuntimeEnv(runtimeEnv).remote().get();
      Assert.assertEquals(val, "B");
    } finally {
      System.clearProperty("ray.job.runtime-env.env-vars.KEY1");
      System.clearProperty("ray.job.runtime-env.env-vars.KEY2");
      Ray.shutdown();
    }
  }

  private static void testDownloadAndLoadPackage(String url) {
    try {
      Ray.init();
      RuntimeEnv runtimeEnv = new RuntimeEnv.Builder().build();
      runtimeEnv.set(JARS, ImmutableList.of(url));
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
      RuntimeEnv runtimeEnv = new RuntimeEnv.Builder().build();
      runtimeEnv.set(JARS, urls);
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
      System.clearProperty("ray.job.runtime-env.jars.0");
      System.clearProperty("ray.job.runtime-env.jars.1");
      Ray.shutdown();
    }
  }

  private static class Pip {
    private String[] packages;
    private Boolean pip_check;

    public String[] getPackages() {
      return packages;
    }

    public void setPackages(String[] packages) {
      this.packages = packages;
    }

    public Boolean getPip_check() {
      return pip_check;
    }

    public void setPip_check(Boolean pip_check) {
      this.pip_check = pip_check;
    }
  }

  public void testRuntimeEnvAPI() {
    try {
      Ray.init();
      RuntimeEnv runtimeEnv = new RuntimeEnv.Builder().build();
      String workingDir = "https://path/to/working_dir.zip";
      runtimeEnv.set("working_dir", workingDir);
      String[] py_modules =
          new String[] {"https://path/to/py_modules1.zip", "https://path/to/py_modules2.zip"};
      runtimeEnv.set("py_modules", py_modules);
      Pip pip = new Pip();
      pip.setPackages(new String[] {"requests", "tensorflow"});
      pip.setPip_check(true);
      runtimeEnv.set("pip", pip);
      String serializedRuntimeEnv = runtimeEnv.serialize();

      RuntimeEnv runtimeEnv2 = RuntimeEnv.deserialize(serializedRuntimeEnv);
      Assert.assertEquals(runtimeEnv2.get("working_dir", String.class), workingDir);
      Assert.assertEquals(runtimeEnv2.get("py_modules", String[].class), py_modules);
      Pip pip2 = runtimeEnv2.get("pip", Pip.class);
      Assert.assertEquals(pip2.getPackages(), pip.getPackages());
      Assert.assertEquals(pip2.getPip_check(), pip.getPip_check());

      Assert.assertEquals(runtimeEnv2.remove("working_dir"), true);
      Assert.assertEquals(runtimeEnv2.remove("py_modules"), true);
      Assert.assertEquals(runtimeEnv2.remove("pip"), true);
      Assert.assertEquals(runtimeEnv2.remove("conda"), false);
      Assert.assertEquals(runtimeEnv2.get("working_dir", String.class), null);
      Assert.assertEquals(runtimeEnv2.get("py_modules", String[].class), null);
      Assert.assertEquals(runtimeEnv2.get("pip", Pip.class), null);
    } finally {
      Ray.shutdown();
    }
  }

  public void testRuntimeEnvJsonStringAPI() {
    try {
      Ray.init();
      RuntimeEnv runtimeEnv = new RuntimeEnv.Builder().build();
      String pipString = "{\"packages\":[\"requests\",\"tensorflow\"],\"pip_check\":false}";
      runtimeEnv.setJsonStr("pip", pipString);
      String serializedRuntimeEnv = runtimeEnv.serialize();

      RuntimeEnv runtimeEnv2 = RuntimeEnv.deserialize(serializedRuntimeEnv);
      Assert.assertEquals(runtimeEnv2.getJsonStr("pip"), pipString);
    } finally {
      Ray.shutdown();
    }
  }

  public void testRuntimeEnvContextForJob() {
    System.setProperty("ray.job.runtime-env.jars.0", FOO_JAR_URL);
    System.setProperty("ray.job.runtime-env.jars.1", BAR_JAR_URL);
    System.setProperty("ray.job.runtime-env.config.setup-timeout-seconds", "1");
    try {
      Ray.init();
      RuntimeEnv runtimeEnv = Ray.getRuntimeContext().getCurrentRuntimeEnv();
      Assert.assertNotNull(runtimeEnv);
      List<String> jars = runtimeEnv.get(JARS, List.class);
      Assert.assertNotNull(jars);
      Assert.assertEquals(jars.size(), 2);
      Assert.assertEquals(jars.get(0), FOO_JAR_URL);
      Assert.assertEquals(jars.get(1), BAR_JAR_URL);
      RuntimeEnvConfig runtimeEnvConfig = runtimeEnv.getConfig();
      Assert.assertNotNull(runtimeEnvConfig);
      Assert.assertEquals((int) runtimeEnvConfig.getSetupTimeoutSeconds(), 1);

    } finally {
      System.clearProperty("ray.job.runtime-env.jars.0");
      System.clearProperty("ray.job.runtime-env.jars.1");
      System.clearProperty("ray.job.runtime-env.config.setup-timeout-seconds");
      Ray.shutdown();
    }
  }

  private static Integer getRuntimeEnvTimeout() {
    RuntimeEnv runtimeEnv = Ray.getRuntimeContext().getCurrentRuntimeEnv();
    if (runtimeEnv != null) {
      return runtimeEnv.getConfig().getSetupTimeoutSeconds();
    }
    return null;
  }

  public void testRuntimeEnvContextForTask() {
    try {
      Ray.init();
      RuntimeEnv currentRuntimeEnv = Ray.getRuntimeContext().getCurrentRuntimeEnv();
      Assert.assertTrue(currentRuntimeEnv.isEmpty());
      RuntimeEnv runtimeEnv = new RuntimeEnv.Builder().build();
      RuntimeEnvConfig runtimeEnvConfig = new RuntimeEnvConfig(1, false);
      runtimeEnv.setConfig(runtimeEnvConfig);
      Integer result =
          Ray.task(RuntimeEnvTest::getRuntimeEnvTimeout).setRuntimeEnv(runtimeEnv).remote().get();
      Assert.assertNotNull(result);
      Assert.assertEquals((int) result, 1);
    } finally {
      Ray.shutdown();
    }
  }
}
