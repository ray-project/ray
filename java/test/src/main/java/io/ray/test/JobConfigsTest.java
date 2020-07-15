package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class JobConfigsTest extends BaseTest {

  @BeforeClass
  public void setupJobConfigs() {
    System.setProperty("ray.job.num-java-workers-per-process", "3");
    System.setProperty("ray.job.jvm-options.0", "-DX=999");
  }

  @AfterClass
  public void tearDownJobConfigs() {
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

  @Test
  public void testJvmOptions() {
    TestUtils.skipTestUnderSingleProcess();
    ObjectRef<String> obj = Ray.task(JobConfigsTest::getJvmOptions).remote();
    Assert.assertEquals("999", obj.get());
  }

  @Test
  public void testNumJavaWorkerPerProcess() {
    TestUtils.skipTestUnderSingleProcess();
    ObjectRef<Integer> obj = Ray.task(JobConfigsTest::getWorkersNum).remote();
    Assert.assertEquals(3, (int) obj.get());
  }


  @Test
  public void testInActor() {
    TestUtils.skipTestUnderSingleProcess();
    ActorHandle<MyActor> actor = Ray.actor(MyActor::new).remote();

    // test jvm options.
    ObjectRef<String> obj1 = actor.task(MyActor::getJvmOptions).remote();
    Assert.assertEquals("999", obj1.get());

    //  test workers number.
    ObjectRef<Integer> obj2 = actor.task(MyActor::getWorkersNum).remote();
    Assert.assertEquals(3, (int) obj2.get());
  }
}
