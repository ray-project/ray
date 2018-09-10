package org.ray.api.test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;
import org.ray.api.function.RayFunc0;
import org.ray.api.function.RayFunc1;
import org.ray.api.function.RayFunc3;
import org.ray.runtime.util.MethodId;
import org.ray.runtime.util.logger.RayLog;

public class LambdaUtilsTest {

  static final String CLASS_NAME = LambdaUtilsTest.class.getName();
  static final Method CALL0;
  static final Method CALL1;
  static final Method CALL2;
  static final Method CALL3;

  static {
    try {
      CALL0 = LambdaUtilsTest.class.getDeclaredMethod("call0", new Class[0]);
      CALL1 = LambdaUtilsTest.class.getDeclaredMethod("call1", new Class[]{Long.class});
      CALL2 = LambdaUtilsTest.class.getDeclaredMethod("call2", new Class[0]);
      CALL3 = LambdaUtilsTest.class
          .getDeclaredMethod("call3", new Class[]{Long.class, String.class});

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static <T0, T1, T2, R0> void testRemoteLambdaParse(RayFunc3<T0, T1, T2, R0> f, int n,
      boolean forceNew, boolean debug)
      throws Exception {
    if (debug) {
      RayLog.core.info("parse#" + f.getClass().getName());
    }
    long start = System.nanoTime();
    for (int i = 0; i < n; i++) {
      MethodId mid = MethodId.fromSerializedLambda(f, forceNew);
    }
    long end = System.nanoTime();
    RayLog.core.info(String.format("remoteLambdaParse(new=%s):total=%sms, one=%s", forceNew,
        TimeUnit.NANOSECONDS.toMillis(end - start),
        (end - start) / n));
  }

  public static <T0, T1, T2, R0> void testRemoteLambdaSerde(RayFunc3<T0, T1, T2, R0> f, int n,
      boolean de, boolean debug)
      throws Exception {
    if (debug) {
      RayLog.core.info("se#" + f.getClass().getName());
    }
    long start = System.nanoTime();
    for (int i = 0; i < n; i++) {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream(1024);
      ObjectOutputStream out = new ObjectOutputStream(bytes);
      out.writeObject(f);
      out.close();
      if (de) {
        ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()));
        RayFunc3 def = (RayFunc3) in.readObject();
        in.close();
        if (debug) {
          RayLog.core.info("de#" + def.getClass().getName());
        }
      }
    }
    long end = System.nanoTime();
    RayLog.core.info(
        String.format("remoteLambdaSer(de=%s):total=%sms,one=%s", de,
            TimeUnit.NANOSECONDS.toMillis(end - start),
            (end - start) / n));
  }

  public static void testCall0(RayFunc0 f) {
    MethodId mid = MethodId.fromSerializedLambda(f);
    RayLog.core.info(mid.toString());
    Assert.assertEquals(mid.load(), CALL0);
    Assert.assertTrue(mid.isStatic);
  }

  public static <T, R> void testCall1(RayFunc1<T, R> f, T t) {
    MethodId mid = MethodId.fromSerializedLambda(f);
    RayLog.core.info(mid.toString());
    Assert.assertEquals(mid.load(), CALL1);
    Assert.assertTrue(mid.isStatic);
  }

  public static <T, R> void testCall2(RayFunc1<T, R> f) {
    MethodId mid = MethodId.fromSerializedLambda(f);
    RayLog.core.info(mid.toString());
    Assert.assertEquals(mid.load(), CALL2);
    Assert.assertTrue(!mid.isStatic);
  }

  public static <T0, T1, T2, R0> void testCall3(RayFunc3<T0, T1, T2, R0> f) {
    MethodId mid = MethodId.fromSerializedLambda(f);
    RayLog.core.info(mid.toString());
    Assert.assertEquals(mid.load(), CALL3);
    Assert.assertTrue(!mid.isStatic);
  }

  public static String call0() {
    long t = System.currentTimeMillis();
    RayLog.core.info("call0:" + t);
    return String.valueOf(t);
  }

  public static String call1(Long v) {
    for (int i = 0; i < 100; i++) {
      v += i;
    }
    RayLog.core.info("call1:" + v);
    return String.valueOf(v);
  }

  @Test
  public void testLambdaSer() throws Exception {
    testCall0(LambdaUtilsTest::call0);
    testCall1(LambdaUtilsTest::call1, Long.valueOf(System.currentTimeMillis()));
    testCall2(LambdaUtilsTest::call2);
    testCall3(LambdaUtilsTest::call3);
  }

  /**
   * to test the serdeLambda's perf.
   */
  public void testBenchmark() throws Exception {
    //test serde
    testRemoteLambdaSerde(LambdaUtilsTest::call3, 2, true, true);
    testRemoteLambdaSerde(LambdaUtilsTest::call3, 2, true, true);
    //warmup

    RayLog.core.info("warmup:serde################");
    testRemoteLambdaSerde(LambdaUtilsTest::call3, 1000000, true, false);
    testRemoteLambdaSerde(LambdaUtilsTest::call3, 1000000, false, false);
    RayLog.core.info("benchmark:serde################");
    testRemoteLambdaSerde(LambdaUtilsTest::call3, 1000000, true, false);
    RayLog.core.info("benchmark:ser################");
    testRemoteLambdaSerde(LambdaUtilsTest::call3, 1000000, false, false);

    //test serde one new call's time, no class cache
    long start = System.nanoTime();
    testRemoteLambdaSerde(LambdaUtilsTest::call3, 1, false, false);
    long end = System.nanoTime();
    RayLog.core.info("one sertime:" + (end - start));

    //test serde one new call's time, no class cache
    start = System.nanoTime();
    testRemoteLambdaSerde(LambdaUtilsTest::call3, 1, false, false);
    end = System.nanoTime();
    RayLog.core.info("one sertime:" + (end - start));

    //test serde one new call's time, no class cache
    start = System.nanoTime();
    testRemoteLambdaSerde(LambdaUtilsTest::call3, 1, false, false);
    end = System.nanoTime();
    RayLog.core.info("one sertime:" + (end - start));

    //test serde one new call's time, no class cache
    start = System.nanoTime();
    testRemoteLambdaSerde(LambdaUtilsTest::call3, 1, true, false);
    end = System.nanoTime();
    RayLog.core.info("one serdetime:" + (end - start));

    //test serde one new call's time, no class cache
    start = System.nanoTime();
    testRemoteLambdaSerde(LambdaUtilsTest::call3, 1, true, false);
    end = System.nanoTime();
    RayLog.core.info("one serdetime:" + (end - start));

    //test serde one new call's time, no class cache
    start = System.nanoTime();
    testRemoteLambdaSerde(LambdaUtilsTest::call3, 1, true, false);
    end = System.nanoTime();
    RayLog.core.info("one serdetime:" + (end - start));

    //test lambda
    testRemoteLambdaParse(LambdaUtilsTest::call3, 2, true, true);
    testRemoteLambdaParse(LambdaUtilsTest::call3, 2, false, true);
    //warmup
    RayLog.core.info("warmup:parse################");
    testRemoteLambdaParse(LambdaUtilsTest::call3, 1000000, true, false);
    testRemoteLambdaParse(LambdaUtilsTest::call3, 1000000, false, false);
    RayLog.core.info("benchmark:parseNew################");
    testRemoteLambdaParse(LambdaUtilsTest::call3, 1000000, true, false);
    RayLog.core.info("benchmark:parseCache################");
    testRemoteLambdaParse(LambdaUtilsTest::call3, 1000000, false, false);
  }

  public String call2() {
    long t = System.currentTimeMillis();
    RayLog.core.info("call2:" + t);
    return "call2:" + t;
  }

  public String call3(Long v, String s) {
    for (int i = 0; i < 100; i++) {
      v += i;
    }
    RayLog.core.info("call3:" + v);
    return String.valueOf(v);
  }
}