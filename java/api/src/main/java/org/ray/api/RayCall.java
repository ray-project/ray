// generated automatically, do not modify.

package org.ray.api;

import org.ray.api.function.*;

/**
 * This class provides type-safe interfaces for Ray.call.
 **/
@SuppressWarnings({"rawtypes", "unchecked"})
class RayCall {
  public static <R> RayObject<R> call(RayFunc0<R> f) {
    Object[] args = new Object[]{};
    return Ray.internal().call(f, args);
  }
  public static <T0, R> RayObject<R> call(RayFunc1<T0, R> f, RayActor<T0> actor) {
    Object[] args = new Object[]{};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, R> RayObject<R> call(RayFunc1<T0, R> f, T0 t0) {
    Object[] args = new Object[]{t0};
    return Ray.internal().call(f, args);
  }
  public static <T0, R> RayObject<R> call(RayFunc1<T0, R> f, RayObject<T0> t0) {
    Object[] args = new Object[]{t0};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, R> RayObject<R> call(RayFunc2<T0, T1, R> f, RayActor<T0> actor, T1 t1) {
    Object[] args = new Object[]{t1};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, R> RayObject<R> call(RayFunc2<T0, T1, R> f, RayActor<T0> actor, RayObject<T1> t1) {
    Object[] args = new Object[]{t1};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, R> RayObject<R> call(RayFunc2<T0, T1, R> f, T0 t0, T1 t1) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, R> RayObject<R> call(RayFunc2<T0, T1, R> f, T0 t0, RayObject<T1> t1) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, R> RayObject<R> call(RayFunc2<T0, T1, R> f, RayObject<T0> t0, T1 t1) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, R> RayObject<R> call(RayFunc2<T0, T1, R> f, RayObject<T0> t0, RayObject<T1> t1) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, R> RayObject<R> call(RayFunc3<T0, T1, T2, R> f, RayActor<T0> actor, T1 t1, T2 t2) {
    Object[] args = new Object[]{t1, t2};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, R> RayObject<R> call(RayFunc3<T0, T1, T2, R> f, RayActor<T0> actor, T1 t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t1, t2};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, R> RayObject<R> call(RayFunc3<T0, T1, T2, R> f, RayActor<T0> actor, RayObject<T1> t1, T2 t2) {
    Object[] args = new Object[]{t1, t2};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, R> RayObject<R> call(RayFunc3<T0, T1, T2, R> f, RayActor<T0> actor, RayObject<T1> t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t1, t2};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, R> RayObject<R> call(RayFunc3<T0, T1, T2, R> f, T0 t0, T1 t1, T2 t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, R> RayObject<R> call(RayFunc3<T0, T1, T2, R> f, T0 t0, T1 t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, R> RayObject<R> call(RayFunc3<T0, T1, T2, R> f, T0 t0, RayObject<T1> t1, T2 t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, R> RayObject<R> call(RayFunc3<T0, T1, T2, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, R> RayObject<R> call(RayFunc3<T0, T1, T2, R> f, RayObject<T0> t0, T1 t1, T2 t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, R> RayObject<R> call(RayFunc3<T0, T1, T2, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, R> RayObject<R> call(RayFunc3<T0, T1, T2, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, R> RayObject<R> call(RayFunc3<T0, T1, T2, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, RayActor<T0> actor, T1 t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t1, t2, t3};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, RayActor<T0> actor, T1 t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t1, t2, t3};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, RayActor<T0> actor, T1 t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t1, t2, t3};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, RayActor<T0> actor, T1 t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t1, t2, t3};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, RayActor<T0> actor, RayObject<T1> t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t1, t2, t3};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, RayActor<T0> actor, RayObject<T1> t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t1, t2, t3};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, RayActor<T0> actor, RayObject<T1> t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t1, t2, t3};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, RayActor<T0> actor, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t1, t2, t3};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, T0 t0, T1 t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayActor<T0> actor, T1 t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayActor<T0> actor, T1 t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayActor<T0> actor, T1 t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayActor<T0> actor, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayActor<T0> actor, T1 t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayActor<T0> actor, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayActor<T0> actor, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayActor<T0> actor, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayActor<T0> actor, RayObject<T1> t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayActor<T0> actor, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayActor<T0> actor, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayActor<T0> actor, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayActor<T0> actor, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayActor<T0> actor, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayActor<T0> actor, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayActor<T0> actor, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayActor<T0> actor, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t1, t2, t3, t4, t5};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayActor<T0> actor, T1 t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t1, t2, t3, t4, t5};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayActor<T0> actor, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t1, t2, t3, t4, t5};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayActor<T0> actor, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t1, t2, t3, t4, t5};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayActor<T0> actor, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t1, t2, t3, t4, t5};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayActor<T0> actor, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t1, t2, t3, t4, t5};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayActor<T0> actor, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t1, t2, t3, t4, t5};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayActor<T0> actor, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t1, t2, t3, t4, t5};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayActor<T0> actor, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t1, t2, t3, t4, t5};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayActor<T0> actor, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t1, t2, t3, t4, t5};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayActor<T0> actor, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t1, t2, t3, t4, t5};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayActor<T0> actor, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t1, t2, t3, t4, t5};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayActor<T0> actor, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t1, t2, t3, t4, t5};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayActor<T0> actor, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t1, t2, t3, t4, t5};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayActor<T0> actor, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t1, t2, t3, t4, t5};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayActor<T0> actor, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t1, t2, t3, t4, t5};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayActor<T0> actor, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t1, t2, t3, t4, t5};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayActor<T0> actor, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t1, t2, t3, t4, t5};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayActor<T0> actor, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t1, t2, t3, t4, t5};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayActor<T0> actor, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t1, t2, t3, t4, t5};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayActor<T0> actor, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t1, t2, t3, t4, t5};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayActor<T0> actor, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t1, t2, t3, t4, t5};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayActor<T0> actor, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t1, t2, t3, t4, t5};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayActor<T0> actor, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t1, t2, t3, t4, t5};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayActor<T0> actor, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t1, t2, t3, t4, t5};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayActor<T0> actor, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t1, t2, t3, t4, t5};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayActor<T0> actor, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t1, t2, t3, t4, t5};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayActor<T0> actor, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t1, t2, t3, t4, t5};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayActor<T0> actor, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t1, t2, t3, t4, t5};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayActor<T0> actor, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t1, t2, t3, t4, t5};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayActor<T0> actor, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t1, t2, t3, t4, t5};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayActor<T0> actor, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t1, t2, t3, t4, t5};
    return Ray.internal().call(f, actor, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args);
  }
}
