/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2018 All Rights Reserved.
 */
package org.ray.api.test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;
import org.ray.api.funcs.RayFunc_0_1;
import org.ray.api.funcs.RayFunc_1_1;
import org.ray.api.funcs.RayFunc_3_1;
import org.ray.util.LambdaUtils;
import org.ray.util.MethodId;

/**
 * @author lin
 * @version $Id: LambdaUtilsTest.java, v 0.1 2018年06月07日 19:35 lin Exp $
 */
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

//  @Test
  public void testLambdaSer() throws Exception {
    testCall0(LambdaUtilsTest::call0);
    testCall1(LambdaUtilsTest::call1, Long.valueOf(System.currentTimeMillis()));
    testCall2(LambdaUtilsTest::call2);
    testCall3(LambdaUtilsTest::call3);
  }

  public static void main(String[] agrs) throws Exception {
    //test serde
    testRemoteLambdaSerde(LambdaUtilsTest::call3, 2, true, true);
    testRemoteLambdaSerde(LambdaUtilsTest::call3, 2, true, true);
    //warmup
    System.out.println("warmup:serde################");
    testRemoteLambdaSerde(LambdaUtilsTest::call3, 1000000, true, false);
    testRemoteLambdaSerde(LambdaUtilsTest::call3, 1000000, false, false);
    System.out.println("benchmark:serde################");
    testRemoteLambdaSerde(LambdaUtilsTest::call3, 1000000, true, false);
    System.out.println("benchmark:ser################");
    testRemoteLambdaSerde(LambdaUtilsTest::call3, 1000000, false, false);

    //test serde one new call's time, no class cache
    long start = System.nanoTime();
    testRemoteLambdaSerde(LambdaUtilsTest::call3, 1, false, false);
    long end = System.nanoTime();
    System.out.println("one sertime:" + (end - start));

    //test serde one new call's time, no class cache
    start = System.nanoTime();
    testRemoteLambdaSerde(LambdaUtilsTest::call3, 1, false, false);
    end = System.nanoTime();
    System.out.println("one sertime:" + (end - start));

    //test serde one new call's time, no class cache
    start = System.nanoTime();
    testRemoteLambdaSerde(LambdaUtilsTest::call3, 1, false, false);
    end = System.nanoTime();
    System.out.println("one sertime:" + (end - start));


    //test serde one new call's time, no class cache
    start = System.nanoTime();
    testRemoteLambdaSerde(LambdaUtilsTest::call3, 1, true, false);
    end = System.nanoTime();
    System.out.println("one serdetime:" + (end - start));

    //test serde one new call's time, no class cache
    start = System.nanoTime();
    testRemoteLambdaSerde(LambdaUtilsTest::call3, 1, true, false);
    end = System.nanoTime();
    System.out.println("one serdetime:" + (end - start));

    //test serde one new call's time, no class cache
    start = System.nanoTime();
    testRemoteLambdaSerde(LambdaUtilsTest::call3, 1, true, false);
    end = System.nanoTime();
    System.out.println("one serdetime:" + (end - start));

    //test lambda
    testRemoteLambdaParse(LambdaUtilsTest::call3, 2, true, true);
    testRemoteLambdaParse(LambdaUtilsTest::call3, 2, false, true);
    //warmup
    System.out.println("warmup:parse################");
    testRemoteLambdaParse(LambdaUtilsTest::call3, 1000000, true, false);
    testRemoteLambdaParse(LambdaUtilsTest::call3, 1000000, false, false);
    System.out.println("benchmark:parseNew################");
    testRemoteLambdaParse(LambdaUtilsTest::call3, 1000000, true, false);
    System.out.println("benchmark:parseCache################");
    testRemoteLambdaParse(LambdaUtilsTest::call3, 1000000, false, false);
  }

  public static <T0, T1, T2, R0> void testRemoteLambdaParse(RayFunc_3_1<T0, T1, T2, R0> f, int n,
      boolean forceNew, boolean debug)
      throws Exception {
    if (debug) {
      System.out.println("parse#" + f.getClass().getName());
    }
    long start = System.nanoTime();
    for (int i = 0; i < n; i++) {
      SerializedLambda lambda = LambdaUtils.getSerializedLambda(f, forceNew);
      MethodId mid = MethodId.fromSerializedLambda(lambda, forceNew);
    }
    long end = System.nanoTime();
    System.out.println(String.format("remoteLambdaParse(new=%s):total=%sms, one=%s", forceNew,
        TimeUnit.NANOSECONDS.toMillis(end - start),
        (end - start) / n));
  }


  public static <T0, T1, T2, R0> void testRemoteLambdaSerde(RayFunc_3_1<T0, T1, T2, R0> f, int n,
      boolean de, boolean debug)
      throws Exception {
    if (debug) {
      System.out.println("se#" + f.getClass().getName());
    }
    long start = System.nanoTime();
    for (int i = 0; i < n; i++) {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream(1024);
      ObjectOutputStream out = new ObjectOutputStream(bytes);
      out.writeObject(f);
      out.close();
      if (de) {
        ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()));
        RayFunc_3_1 def = (RayFunc_3_1) in.readObject();
        in.close();
        if (debug) {
          System.out.println("de#" + def.getClass().getName());
        }
      }
    }
    long end = System.nanoTime();
    System.out.println(
        String.format("remoteLambdaSer(de=%s):total=%sms,one=%s", de,
            TimeUnit.NANOSECONDS.toMillis(end - start),
            (end - start) / n));
  }


  public static void testCall0(RayFunc_0_1 f) {
    SerializedLambda lambda = LambdaUtils.getSerializedLambda(f);
    MethodId mid = MethodId.fromSerializedLambda(lambda);
    System.out.println(mid);
    Assert.assertEquals(mid.load(), CALL0);
    Assert.assertTrue(mid.isStatic());
  }

  public static <T, R> void testCall1(RayFunc_1_1<T, R> f, T t) {
    SerializedLambda lambda = LambdaUtils.getSerializedLambda(f);
    MethodId mid = MethodId.fromSerializedLambda(lambda);
    System.out.println(mid);
    Assert.assertEquals(mid.load(), CALL1);
    Assert.assertTrue(mid.isStatic());
  }

  public static <T, R> void testCall2(RayFunc_1_1<T, R> f) {
    SerializedLambda lambda = LambdaUtils.getSerializedLambda(f);
    MethodId mid = MethodId.fromSerializedLambda(lambda);
    System.out.println(mid);
    Assert.assertEquals(mid.load(), CALL2);
    Assert.assertTrue(!mid.isStatic());
  }


  public static <T0, T1, T2, R0> void testCall3(RayFunc_3_1<T0, T1, T2, R0> f) {
    SerializedLambda lambda = LambdaUtils.getSerializedLambda(f);
    MethodId mid = MethodId.fromSerializedLambda(lambda);
    System.out.println(mid);
    Assert.assertEquals(mid.load(), CALL3);
    Assert.assertTrue(!mid.isStatic());
  }

  public static String call0() {
    long t = System.currentTimeMillis();
    System.out.println("call0:" + t);
    return String.valueOf(t);
  }

  public static String call1(Long v) {
    for (int i = 0; i < 100; i++) {
      v += i;
    }
    System.out.println("call1:" + v);
    return String.valueOf(v);
  }

  public String call2() {
    long t = System.currentTimeMillis();
    System.out.println("call2:" + t);
    return "call2:" + t;
  }

  public String call3(Long v, String s) {
    for (int i = 0; i < 100; i++) {
      v += i;
    }
    System.out.println("call3:" + v);
    return String.valueOf(v);
  }
}