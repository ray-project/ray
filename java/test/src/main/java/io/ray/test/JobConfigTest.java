package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class JobConfigTest extends BaseTest {

  @BeforeClass
  public void setupJobConfig() {
    System.setProperty("ray.raylet.config.enable_multi_tenancy", "true");
    System.setProperty("ray.job.num-java-workers-per-process", "3");
    System.setProperty("ray.job.jvm-options.0", "-DX=999");
  }

  @AfterClass
  public void tearDownJobConfig() {
    System.clearProperty("ray.raylet.config.enable_multi_tenancy");
    System.clearProperty("ray.job.num-java-workers-per-process");
    System.clearProperty("ray.job.jvm-options.0");
  }

  public static String getJvmOptions() {
    return System.getProperty("X");
  }

  public static Integer getWorkersNum() {
    return TestUtils.getRuntime().getRayConfig().numWorkersPerProcess;
  }

  public static class MyActor {

    public Integer getWorkersNum() {
      return TestUtils.getRuntime().getRayConfig().numWorkersPerProcess;
    }

    public String getJvmOptions() {
      return System.getProperty("X");
    }
  }

  public void testJvmOptions() {
    ObjectRef<String> obj = Ray.task(JobConfigTest::getJvmOptions).remote();
    Assert.assertEquals("999", obj.get());
  }

  public void testNumJavaWorkerPerProcess() {
    ObjectRef<Integer> obj = Ray.task(JobConfigTest::getWorkersNum).remote();
    Assert.assertEquals(3, (int) obj.get());
  }


  public void testInActor() {
    ActorHandle<MyActor> actor = Ray.actor(MyActor::new).remote();

    // test jvm options.
    ObjectRef<String> obj1 = actor.task(MyActor::getJvmOptions).remote();
    Assert.assertEquals("999", obj1.get());

    //  test workers number.
    ObjectRef<Integer> obj2 = actor.task(MyActor::getWorkersNum).remote();
    Assert.assertEquals(3, (int) obj2.get());
  }
}
