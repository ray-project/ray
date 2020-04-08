package io.ray.api.test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.ray.api.Ray;
import io.ray.api.TestUtils;
import io.ray.api.id.ObjectId;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test Ray.call API
 */
public class RayCallTest extends BaseTest {

  private static int testInt(int val) {
    return val;
  }

  private static byte testByte(byte val) {
    return val;
  }

  private static short testShort(short val) {
    return val;
  }

  private static long testLong(long val) {
    return val;
  }

  private static double testDouble(double val) {
    return val;
  }

  private static float testFloat(float val) {
    return val;
  }

  private static boolean testBool(boolean val) {
    return val;
  }

  private static String testString(String val) {
    return val;
  }

  private static List<Integer> testList(List<Integer> val) {
    return val;
  }

  private static Map<String, Integer> testMap(Map<String, Integer> val) {
    return val;
  }

  private static TestUtils.LargeObject testLargeObject(TestUtils.LargeObject largeObject) {
    return largeObject;
  }

  private static void testNoReturn(ObjectId objectId) {
    // Put an object in object store to inform driver that this function is executing.
    TestUtils.getRuntime().getObjectStore().put(1, objectId);
  }

  /**
   * Test calling and returning different types.
   */
  @Test
  public void testType() {
    Assert.assertEquals(1, (int) Ray.call(RayCallTest::testInt, 1).get());
    Assert.assertEquals(1, (byte) Ray.call(RayCallTest::testByte, (byte) 1).get());
    Assert.assertEquals(1, (short) Ray.call(RayCallTest::testShort, (short) 1).get());
    Assert.assertEquals(1, (long) Ray.call(RayCallTest::testLong, 1L).get());
    Assert.assertEquals(1.0, Ray.call(RayCallTest::testDouble, 1.0).get(), 0.0);
    Assert.assertEquals(1.0f, Ray.call(RayCallTest::testFloat, 1.0f).get(), 0.0);
    Assert.assertTrue(Ray.call(RayCallTest::testBool, true).get());
    Assert.assertEquals("foo", Ray.call(RayCallTest::testString, "foo").get());
    List<Integer> list = ImmutableList.of(1, 2, 3);
    Assert.assertEquals(list, Ray.call(RayCallTest::testList, list).get());
    Map<String, Integer> map = ImmutableMap.of("1", 1, "2", 2);
    Assert.assertEquals(map, Ray.call(RayCallTest::testMap, map).get());
    TestUtils.LargeObject largeObject = new TestUtils.LargeObject();
    Assert.assertNotNull(Ray.call(RayCallTest::testLargeObject, largeObject).get());

    ObjectId randomObjectId = ObjectId.fromRandom();
    Ray.call(RayCallTest::testNoReturn, randomObjectId);
    Assert.assertEquals(((int) Ray.get(randomObjectId, Integer.class)), 1);
  }

  private static int testNoParam() {
    return 0;
  }

  private static int testOneParam(int a) {
    return a;
  }

  private static int testTwoParams(int a, int b) {
    return a + b;
  }

  private static int testThreeParams(int a, int b, int c) {
    return a + b + c;
  }

  private static int testFourParams(int a, int b, int c, int d) {
    return a + b + c + d;
  }

  private static int testFiveParams(int a, int b, int c, int d, int e) {
    return a + b + c + d + e;
  }

  private static int testSixParams(int a, int b, int c, int d, int e, int f) {
    return a + b + c + d + e + f;
  }

  @Test
  public void testNumberOfParameters() {
    Assert.assertEquals(0, (int) Ray.call(RayCallTest::testNoParam).get());
    Assert.assertEquals(1, (int) Ray.call(RayCallTest::testOneParam, 1).get());
    Assert.assertEquals(2, (int) Ray.call(RayCallTest::testTwoParams, 1, 1).get());
    Assert.assertEquals(3, (int) Ray.call(RayCallTest::testThreeParams, 1, 1, 1).get());
    Assert.assertEquals(4, (int) Ray.call(RayCallTest::testFourParams, 1, 1, 1, 1).get());
    Assert.assertEquals(5, (int) Ray.call(RayCallTest::testFiveParams, 1, 1, 1, 1, 1).get());
    Assert.assertEquals(6, (int) Ray.call(RayCallTest::testSixParams, 1, 1, 1, 1, 1, 1).get());
  }

}
