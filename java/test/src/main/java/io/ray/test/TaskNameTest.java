package io.ray.test;

import io.ray.api.Ray;
import org.testng.Assert;
import org.testng.annotations.Test;

/** Task Name Test. */
public class TaskNameTest extends BaseTest {

  private static int testFoo() {
    return 0;
  }

  /** Test setting task name at task submission time. */
  @Test
  public void testSetName() {
    Assert.assertEquals(0, (int) Ray.task(TaskNameTest::testFoo).setName("foo").remote().get());
  }
}
