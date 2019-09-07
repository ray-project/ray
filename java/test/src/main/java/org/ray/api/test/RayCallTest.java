package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.ray.api.Ray;
import org.ray.api.annotation.RayRemote;
import org.ray.api.id.ObjectId;
import org.ray.runtime.AbstractRayRuntime;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test Ray.call API
 */
public class RayCallTest extends BaseTest {

  @RayRemote
  private static int testInt(int val) {
    return val;
  }

  @RayRemote
  private static byte testByte(byte val) {
    return val;
  }

  @RayRemote
  private static short testShort(short val) {
    return val;
  }

  @RayRemote
  private static long testLong(long val) {
    return val;
  }

  @RayRemote
  private static double testDouble(double val) {
    return val;
  }

  @RayRemote
  private static float testFloat(float val) {
    return val;
  }

  @RayRemote
  private static boolean testBool(boolean val) {
    return val;
  }

  @RayRemote
  private static String testString(String val) {
    return val;
  }

  @RayRemote
  private static List<Integer> testList(List<Integer> val) {
    return val;
  }

  @RayRemote
  private static Map<String, Integer> testMap(Map<String, Integer> val) {
    return val;
  }

  public static class LargeObject implements Serializable {

    private byte[] data = new byte[1024 * 1024];
  }

  @RayRemote
  private static LargeObject testLargeObject(LargeObject largeObject) {
    return largeObject;
  }

  @RayRemote
  private static void testNoReturn(ObjectId objectId) {
    // Put an object in object store to inform driver that this function is executing.
    ((AbstractRayRuntime) Ray.internal()).getObjectStore().put(1, objectId);
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
    LargeObject largeObject = new LargeObject();
    Assert.assertNotNull(Ray.call(RayCallTest::testLargeObject, largeObject).get());

    ObjectId randomObjectId = ObjectId.fromRandom();
    Ray.call(RayCallTest::testNoReturn, randomObjectId);
    Assert.assertEquals(((int) Ray.get(randomObjectId)), 1);
  }

  @RayRemote
  private static int testNoParam() {
    return 0;
  }

  @RayRemote
  private static int testOneParam(int a) {
    return a;
  }

  @RayRemote
  private static int testTwoParams(int a, int b) {
    return a + b;
  }

  @RayRemote
  private static int testThreeParams(int a, int b, int c) {
    return a + b + c;
  }

  @RayRemote
  private static int testFourParams(int a, int b, int c, int d) {
    return a + b + c + d;
  }

  @RayRemote
  private static int testFiveParams(int a, int b, int c, int d, int e) {
    return a + b + c + d + e;
  }

  @RayRemote
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
