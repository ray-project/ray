package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class JobConfigTest extends BaseTest {

  @BeforeClass
  public void setupJobConfig() {
    System.setProperty("ray.raylet.startup-token", "0");
    System.setProperty("ray.job.jvm-options.0", "-DX=999");
    System.setProperty("ray.job.jvm-options.1", "-DY=998");
  }

  public static String getJvmOptions(String propertyName) {
    return System.getProperty(propertyName);
  }

  public static class MyActor {

    public String getJvmOptions(String propertyName) {
      return System.getProperty(propertyName);
    }
  }

  public void testJvmOptions() {
    Assert.assertEquals("999", Ray.task(JobConfigTest::getJvmOptions, "X").remote().get());
    Assert.assertEquals("998", Ray.task(JobConfigTest::getJvmOptions, "Y").remote().get());
  }

  public void testInActor() {
    ActorHandle<MyActor> actor = Ray.actor(MyActor::new).remote();

    // test jvm options.
    Assert.assertEquals("999", actor.task(MyActor::getJvmOptions, "X").remote().get());
    Assert.assertEquals("998", actor.task(MyActor::getJvmOptions, "Y").remote().get());
  }
}
