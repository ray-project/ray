package io.ray.test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.ray.api.Ray;
import io.ray.api.id.ObjectId;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

/** Test Ray.call API */
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

  private static ByteBuffer testByteBuffer(ByteBuffer buffer) {
    return buffer;
  }

  /** Test calling and returning different types. */
  @Test
  public void testType() {
    Assert.assertEquals(1, (int) Ray.task(RayCallTest::testInt, 1).remote().get());
    Assert.assertEquals(1, (byte) Ray.task(RayCallTest::testByte, (byte) 1).remote().get());
    Assert.assertEquals(1, (short) Ray.task(RayCallTest::testShort, (short) 1).remote().get());
    Assert.assertEquals(1, (long) Ray.task(RayCallTest::testLong, 1L).remote().get());
    Assert.assertEquals(1.0, Ray.task(RayCallTest::testDouble, 1.0).remote().get(), 0.0);
    Assert.assertEquals(1.0f, Ray.task(RayCallTest::testFloat, 1.0f).remote().get(), 0.0);
    Assert.assertTrue(Ray.task(RayCallTest::testBool, true).remote().get());
    Assert.assertEquals("foo", Ray.task(RayCallTest::testString, "foo").remote().get());
    List<Integer> list = ImmutableList.of(1, 2, 3);
    Assert.assertEquals(list, Ray.task(RayCallTest::testList, list).remote().get());
    Map<String, Integer> map = ImmutableMap.of("1", 1, "2", 2);
    Assert.assertEquals(map, Ray.task(RayCallTest::testMap, map).remote().get());
    TestUtils.LargeObject largeObject = new TestUtils.LargeObject();
    Assert.assertNotNull(Ray.task(RayCallTest::testLargeObject, largeObject).remote().get());
    ByteBuffer buffer1 = ByteBuffer.wrap("foo".getBytes(StandardCharsets.UTF_8));
    ByteBuffer buffer2 = Ray.task(RayCallTest::testByteBuffer, buffer1).remote().get();
    byte[] bytes = new byte[buffer2.remaining()];
    buffer2.get(bytes);
    Assert.assertEquals("foo", new String(bytes, StandardCharsets.UTF_8));

    // TODO(edoakes): this test doesn't work now that we've switched to direct call
    // mode. To make it work, we need to implement the same protocol for resolving
    // passed ObjectIDs that we have in Python.
    // ObjectId randomObjectId = ObjectId.fromRandom();
    // Ray.task(RayCallTest::testNoReturn, randomObjectId).remote();
    // Assert.assertEquals(((int) Ray.get(randomObjectId, Integer.class)), 1);
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
    Assert.assertEquals(0, (int) Ray.task(RayCallTest::testNoParam).remote().get());
    Assert.assertEquals(1, (int) Ray.task(RayCallTest::testOneParam, 1).remote().get());
    Assert.assertEquals(2, (int) Ray.task(RayCallTest::testTwoParams, 1, 1).remote().get());
    Assert.assertEquals(3, (int) Ray.task(RayCallTest::testThreeParams, 1, 1, 1).remote().get());
    Assert.assertEquals(4, (int) Ray.task(RayCallTest::testFourParams, 1, 1, 1, 1).remote().get());
    Assert.assertEquals(
        5, (int) Ray.task(RayCallTest::testFiveParams, 1, 1, 1, 1, 1).remote().get());
    Assert.assertEquals(
        6, (int) Ray.task(RayCallTest::testSixParams, 1, 1, 1, 1, 1, 1).remote().get());
  }
}
