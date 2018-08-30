// generated automatically, do not modify.

package org.ray.api;

import org.ray.api.funcs.*;

@SuppressWarnings({"rawtypes", "unchecked"})
class Rpc {
  public static <R> RayObject<R> call(RayFunc0<R> f) {
    return Ray.internal().call(f);
  }
  public static <T1, R> RayObject<R> call(RayFunc1<T1, R> f, T1 t1) {
    return Ray.internal().call(f, t1);
  }
  public static <T1, R> RayObject<R> call(RayFunc1<T1, R> f, RayObject<T1> t1) {
    return Ray.internal().call(f, t1);
  }
  public static <T1, T2, R> RayObject<R> call(RayFunc2<T1, T2, R> f, T1 t1, T2 t2) {
    return Ray.internal().call(f, t1, t2);
  }
  public static <T1, T2, R> RayObject<R> call(RayFunc2<T1, T2, R> f, T1 t1, RayObject<T2> t2) {
    return Ray.internal().call(f, t1, t2);
  }
  public static <T1, T2, R> RayObject<R> call(RayFunc2<T1, T2, R> f, RayObject<T1> t1, T2 t2) {
    return Ray.internal().call(f, t1, t2);
  }
  public static <T1, T2, R> RayObject<R> call(RayFunc2<T1, T2, R> f, RayObject<T1> t1, RayObject<T2> t2) {
    return Ray.internal().call(f, t1, t2);
  }
  public static <T1, T2, T3, R> RayObject<R> call(RayFunc3<T1, T2, T3, R> f, T1 t1, T2 t2, T3 t3) {
    return Ray.internal().call(f, t1, t2, t3);
  }
  public static <T1, T2, T3, R> RayObject<R> call(RayFunc3<T1, T2, T3, R> f, T1 t1, T2 t2, RayObject<T3> t3) {
    return Ray.internal().call(f, t1, t2, t3);
  }
  public static <T1, T2, T3, R> RayObject<R> call(RayFunc3<T1, T2, T3, R> f, T1 t1, RayObject<T2> t2, T3 t3) {
    return Ray.internal().call(f, t1, t2, t3);
  }
  public static <T1, T2, T3, R> RayObject<R> call(RayFunc3<T1, T2, T3, R> f, T1 t1, RayObject<T2> t2, RayObject<T3> t3) {
    return Ray.internal().call(f, t1, t2, t3);
  }
  public static <T1, T2, T3, R> RayObject<R> call(RayFunc3<T1, T2, T3, R> f, RayObject<T1> t1, T2 t2, T3 t3) {
    return Ray.internal().call(f, t1, t2, t3);
  }
  public static <T1, T2, T3, R> RayObject<R> call(RayFunc3<T1, T2, T3, R> f, RayObject<T1> t1, T2 t2, RayObject<T3> t3) {
    return Ray.internal().call(f, t1, t2, t3);
  }
  public static <T1, T2, T3, R> RayObject<R> call(RayFunc3<T1, T2, T3, R> f, RayObject<T1> t1, RayObject<T2> t2, T3 t3) {
    return Ray.internal().call(f, t1, t2, t3);
  }
  public static <T1, T2, T3, R> RayObject<R> call(RayFunc3<T1, T2, T3, R> f, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3) {
    return Ray.internal().call(f, t1, t2, t3);
  }
  public static <T1, T2, T3, T4, R> RayObject<R> call(RayFunc4<T1, T2, T3, T4, R> f, T1 t1, T2 t2, T3 t3, T4 t4) {
    return Ray.internal().call(f, t1, t2, t3, t4);
  }
  public static <T1, T2, T3, T4, R> RayObject<R> call(RayFunc4<T1, T2, T3, T4, R> f, T1 t1, T2 t2, T3 t3, RayObject<T4> t4) {
    return Ray.internal().call(f, t1, t2, t3, t4);
  }
  public static <T1, T2, T3, T4, R> RayObject<R> call(RayFunc4<T1, T2, T3, T4, R> f, T1 t1, T2 t2, RayObject<T3> t3, T4 t4) {
    return Ray.internal().call(f, t1, t2, t3, t4);
  }
  public static <T1, T2, T3, T4, R> RayObject<R> call(RayFunc4<T1, T2, T3, T4, R> f, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    return Ray.internal().call(f, t1, t2, t3, t4);
  }
  public static <T1, T2, T3, T4, R> RayObject<R> call(RayFunc4<T1, T2, T3, T4, R> f, T1 t1, RayObject<T2> t2, T3 t3, T4 t4) {
    return Ray.internal().call(f, t1, t2, t3, t4);
  }
  public static <T1, T2, T3, T4, R> RayObject<R> call(RayFunc4<T1, T2, T3, T4, R> f, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    return Ray.internal().call(f, t1, t2, t3, t4);
  }
  public static <T1, T2, T3, T4, R> RayObject<R> call(RayFunc4<T1, T2, T3, T4, R> f, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    return Ray.internal().call(f, t1, t2, t3, t4);
  }
  public static <T1, T2, T3, T4, R> RayObject<R> call(RayFunc4<T1, T2, T3, T4, R> f, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    return Ray.internal().call(f, t1, t2, t3, t4);
  }
  public static <T1, T2, T3, T4, R> RayObject<R> call(RayFunc4<T1, T2, T3, T4, R> f, RayObject<T1> t1, T2 t2, T3 t3, T4 t4) {
    return Ray.internal().call(f, t1, t2, t3, t4);
  }
  public static <T1, T2, T3, T4, R> RayObject<R> call(RayFunc4<T1, T2, T3, T4, R> f, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4) {
    return Ray.internal().call(f, t1, t2, t3, t4);
  }
  public static <T1, T2, T3, T4, R> RayObject<R> call(RayFunc4<T1, T2, T3, T4, R> f, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4) {
    return Ray.internal().call(f, t1, t2, t3, t4);
  }
  public static <T1, T2, T3, T4, R> RayObject<R> call(RayFunc4<T1, T2, T3, T4, R> f, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    return Ray.internal().call(f, t1, t2, t3, t4);
  }
  public static <T1, T2, T3, T4, R> RayObject<R> call(RayFunc4<T1, T2, T3, T4, R> f, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4) {
    return Ray.internal().call(f, t1, t2, t3, t4);
  }
  public static <T1, T2, T3, T4, R> RayObject<R> call(RayFunc4<T1, T2, T3, T4, R> f, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    return Ray.internal().call(f, t1, t2, t3, t4);
  }
  public static <T1, T2, T3, T4, R> RayObject<R> call(RayFunc4<T1, T2, T3, T4, R> f, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    return Ray.internal().call(f, t1, t2, t3, t4);
  }
  public static <T1, T2, T3, T4, R> RayObject<R> call(RayFunc4<T1, T2, T3, T4, R> f, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    return Ray.internal().call(f, t1, t2, t3, t4);
  }
  public static <T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc5<T1, T2, T3, T4, T5, R> f, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5);
  }
  public static <T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc5<T1, T2, T3, T4, T5, R> f, T1 t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5);
  }
  public static <T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc5<T1, T2, T3, T4, T5, R> f, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5);
  }
  public static <T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc5<T1, T2, T3, T4, T5, R> f, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5);
  }
  public static <T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc5<T1, T2, T3, T4, T5, R> f, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5);
  }
  public static <T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc5<T1, T2, T3, T4, T5, R> f, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5);
  }
  public static <T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc5<T1, T2, T3, T4, T5, R> f, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5);
  }
  public static <T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc5<T1, T2, T3, T4, T5, R> f, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5);
  }
  public static <T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc5<T1, T2, T3, T4, T5, R> f, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5);
  }
  public static <T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc5<T1, T2, T3, T4, T5, R> f, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5);
  }
  public static <T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc5<T1, T2, T3, T4, T5, R> f, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5);
  }
  public static <T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc5<T1, T2, T3, T4, T5, R> f, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5);
  }
  public static <T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc5<T1, T2, T3, T4, T5, R> f, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5);
  }
  public static <T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc5<T1, T2, T3, T4, T5, R> f, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5);
  }
  public static <T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc5<T1, T2, T3, T4, T5, R> f, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5);
  }
  public static <T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc5<T1, T2, T3, T4, T5, R> f, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5);
  }
  public static <T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc5<T1, T2, T3, T4, T5, R> f, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, T5 t5) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5);
  }
  public static <T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc5<T1, T2, T3, T4, T5, R> f, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5);
  }
  public static <T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc5<T1, T2, T3, T4, T5, R> f, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5);
  }
  public static <T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc5<T1, T2, T3, T4, T5, R> f, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5);
  }
  public static <T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc5<T1, T2, T3, T4, T5, R> f, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5);
  }
  public static <T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc5<T1, T2, T3, T4, T5, R> f, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5);
  }
  public static <T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc5<T1, T2, T3, T4, T5, R> f, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5);
  }
  public static <T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc5<T1, T2, T3, T4, T5, R> f, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5);
  }
  public static <T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc5<T1, T2, T3, T4, T5, R> f, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5);
  }
  public static <T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc5<T1, T2, T3, T4, T5, R> f, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5);
  }
  public static <T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc5<T1, T2, T3, T4, T5, R> f, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5);
  }
  public static <T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc5<T1, T2, T3, T4, T5, R> f, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5);
  }
  public static <T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc5<T1, T2, T3, T4, T5, R> f, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5);
  }
  public static <T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc5<T1, T2, T3, T4, T5, R> f, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5);
  }
  public static <T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc5<T1, T2, T3, T4, T5, R> f, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5);
  }
  public static <T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc5<T1, T2, T3, T4, T5, R> f, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, RayObject<T6> t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, T1 t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5, T6 t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, T1 t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5, RayObject<T6> t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5, T6 t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5, RayObject<T6> t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, T6 t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, RayObject<T6> t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5, T6 t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5, RayObject<T6> t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, T6 t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, RayObject<T6> t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, T6 t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, RayObject<T6> t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, T6 t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, RayObject<T6> t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5, T6 t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5, RayObject<T6> t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5, T6 t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5, RayObject<T6> t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5, T6 t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5, RayObject<T6> t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, T6 t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, RayObject<T6> t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5, T6 t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5, RayObject<T6> t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, T6 t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, RayObject<T6> t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, T6 t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, RayObject<T6> t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, T6 t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, RayObject<T6> t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, T5 t5, RayObject<T6> t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5, T6 t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5, RayObject<T6> t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5, T6 t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5, RayObject<T6> t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, T6 t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, RayObject<T6> t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5, T6 t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5, RayObject<T6> t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, T6 t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, RayObject<T6> t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, T6 t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, RayObject<T6> t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, T6 t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, RayObject<T6> t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5, T6 t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5, RayObject<T6> t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5, T6 t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5, RayObject<T6> t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5, T6 t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5, RayObject<T6> t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, T6 t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, RayObject<T6> t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5, T6 t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5, RayObject<T6> t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, T6 t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, RayObject<T6> t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, T6 t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, RayObject<T6> t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, T6 t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
  public static <T1, T2, T3, T4, T5, T6, R> RayObject<R> call(RayFunc6<T1, T2, T3, T4, T5, T6, R> f, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, RayObject<T6> t6) {
    return Ray.internal().call(f, t1, t2, t3, t4, t5, t6);
  }
}
