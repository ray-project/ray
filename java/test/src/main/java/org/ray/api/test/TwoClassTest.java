package org.ray.api.test;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ray.api.Ray;

/**
 * local test in IDE, for class lazy load.
 */
@RunWith(MyRunner.class)
public class TwoClassTest {

  @Test
  public void testLocal() {
    Assert.assertTrue(
        Ray.call(TypesTest::sayBool).get());//call function in other class which may not be loaded
  }

}
