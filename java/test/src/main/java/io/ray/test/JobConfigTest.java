package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class JobConfigTest extends BaseTest {

  private String oldNumWorkersPerProcess;

  @BeforeClass
  public void setupJobConfig() {
    oldNumWorkersPerProcess = System.getProperty("ray.job.num-java-workers-per-process");
    System.setProperty("ray.job.num-java-workers-per-process", "3");
    System.setProperty("ray.job.jvm-options.0", "-DX=999");
    System.setProperty("ray.job.jvm-options.1", "-DY=998");
    System.setProperty("ray.job.worker-env.foo1", "bar1");
    System.setProperty("ray.job.worker-env.foo2", "bar2");
  }

  @AfterClass
  public void tearDownJobConfig() {
    System.setProperty("ray.job.num-java-workers-per-process", oldNumWorkersPerProcess);
    System.clearProperty("ray.job.jvm-options.0");
    System.clearProperty("ray.job.jvm-options.1");
    System.clearProperty("ray.job.worker-env.foo1");
    System.clearProperty("ray.job.worker-env.foo2");
  }

  public static String getJvmOptions(String propertyName) {
    return System.getProperty(propertyName);
  }

  public static String getEnvVariable(String key) {
    return System.getenv(key);
  }

  public static class MyActor {

    public String getJvmOptions(String propertyName) {
      return System.getProperty(propertyName);
    }

    public static String getEnvVariable(String key) {
      return System.getenv(key);
    }
  }

  public void testJvmOptions() {
    Assert.assertEquals("999", Ray.task(JobConfigTest::getJvmOptions, "X").remote().get());
    Assert.assertEquals("998", Ray.task(JobConfigTest::getJvmOptions, "Y").remote().get());
  }

  public void testWorkerEnvVariable() {
    Assert.assertEquals("bar1", Ray.task(JobConfigTest::getEnvVariable, "foo1").remote().get());
    Assert.assertEquals("bar2", Ray.task(JobConfigTest::getEnvVariable, "foo2").remote().get());
  }

  public void testNumJavaWorkersPerProcess() {
    Assert.assertEquals(TestUtils.getNumWorkersPerProcess(), 3);
  }

  public void testInActor() {
    ActorHandle<MyActor> actor = Ray.actor(MyActor::new).remote();

    // test jvm options.
    Assert.assertEquals("999", actor.task(MyActor::getJvmOptions, "X").remote().get());
    Assert.assertEquals("998", actor.task(MyActor::getJvmOptions, "Y").remote().get());

    // test worker env variables
    Assert.assertEquals("bar1", Ray.task(MyActor::getEnvVariable, "foo1").remote().get());
    Assert.assertEquals("bar2", Ray.task(MyActor::getEnvVariable, "foo2").remote().get());
  }
}
