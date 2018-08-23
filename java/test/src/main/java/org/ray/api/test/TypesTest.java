package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ray.api.Ray;
import org.ray.api.RayRemote;

/**
 * types test.
 */
@RunWith(MyRunner.class)
public class TypesTest {

  @RayRemote
  private static int testInt() {
    return 1;
  }

  @RayRemote
  private static byte testByte() {
    return 1;
  }

  @RayRemote
  private static short testShort() {
    return 1;
  }

  @RayRemote
  private static long testLong() {
    return 1;
  }

  @RayRemote
  private static double testDouble() {
    return 1;
  }

  @RayRemote
  private static float testFloat() {
    return 1;
  }

  @RayRemote
  private static boolean testBool() {
    return true;
  }

  @RayRemote
  private static String testString() {
    return "foo";
  }

  @RayRemote
  private static List<Integer> testList() {
    return ImmutableList.of(1, 2, 3);
  }

  @RayRemote
  private static Map<String, Integer> testMap() {
    return ImmutableMap.of("1", 1, "2", 2);
  }

  @Test
  public void test() {
    Assert.assertEquals(1, (int) Ray.call(TypesTest::testInt).get());
    Assert.assertEquals(1, (byte) Ray.call(TypesTest::testByte).get());
    Assert.assertEquals(1, (short) Ray.call(TypesTest::testShort).get());
    Assert.assertEquals(1, (long) Ray.call(TypesTest::testLong).get());
    Assert.assertEquals(1.0, Ray.call(TypesTest::testDouble).get(), 0.0);
    Assert.assertEquals(1.0f, Ray.call(TypesTest::testFloat).get(), 0.0);
    Assert.assertEquals(true, Ray.call(TypesTest::testBool).get());
    Assert.assertEquals("foo", Ray.call(TypesTest::testString).get());
    Assert.assertEquals(ImmutableList.of(1, 2, 3), Ray.call(TypesTest::testList).get());
    Assert.assertEquals(ImmutableMap.of("1", 1, "2", 2), Ray.call(TypesTest::testMap).get());
  }
}
