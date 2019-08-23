// generated automatically, do not modify.

package org.ray.api;

import org.ray.api.function.RayFunc0;
import org.ray.api.function.RayFunc1;
import org.ray.api.function.RayFunc2;
import org.ray.api.function.RayFunc3;
import org.ray.api.function.RayFunc4;
import org.ray.api.function.RayFunc5;
import org.ray.api.function.RayFunc6;
import org.ray.api.function.RayFuncVoid0;
import org.ray.api.function.RayFuncVoid1;
import org.ray.api.function.RayFuncVoid2;
import org.ray.api.function.RayFuncVoid3;
import org.ray.api.function.RayFuncVoid4;
import org.ray.api.function.RayFuncVoid5;
import org.ray.api.function.RayFuncVoid6;
import org.ray.api.options.ActorCreationOptions;
import org.ray.api.options.CallOptions;

/**
 * This class provides type-safe interfaces for `Ray.call` and `Ray.createActor`.
 **/
@SuppressWarnings({"rawtypes", "unchecked"})
class RayCall {
  // =======================================
  // Methods for remote function invocation.
  // =======================================
  public static <R> RayObject<R> call(RayFunc0<R> f) {
    Object[] args = new Object[]{};
    return Ray.internal().call(f, args, null);
  }
  public static <R> RayObject<R> call(RayFunc0<R> f, CallOptions options) {
    Object[] args = new Object[]{};
    return Ray.internal().call(f, args, options);
  }
  public static void call(RayFuncVoid0 f) {
    Object[] args = new Object[]{};
    Ray.internal().call(f, args, null);
  }
  public static void call(RayFuncVoid0 f, CallOptions options) {
    Object[] args = new Object[]{};
    Ray.internal().call(f, args, options);
  }
  public static <T0, R> RayObject<R> call(RayFunc1<T0, R> f, T0 t0) {
    Object[] args = new Object[]{t0};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, R> RayObject<R> call(RayFunc1<T0, R> f, RayObject<T0> t0) {
    Object[] args = new Object[]{t0};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, R> RayObject<R> call(RayFunc1<T0, R> f, T0 t0, CallOptions options) {
    Object[] args = new Object[]{t0};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, R> RayObject<R> call(RayFunc1<T0, R> f, RayObject<T0> t0, CallOptions options) {
    Object[] args = new Object[]{t0};
    return Ray.internal().call(f, args, options);
  }
  public static <T0> void call(RayFuncVoid1<T0> f, T0 t0) {
    Object[] args = new Object[]{t0};
    Ray.internal().call(f, args, null);
  }
  public static <T0> void call(RayFuncVoid1<T0> f, RayObject<T0> t0) {
    Object[] args = new Object[]{t0};
    Ray.internal().call(f, args, null);
  }
  public static <T0> void call(RayFuncVoid1<T0> f, T0 t0, CallOptions options) {
    Object[] args = new Object[]{t0};
    Ray.internal().call(f, args, options);
  }
  public static <T0> void call(RayFuncVoid1<T0> f, RayObject<T0> t0, CallOptions options) {
    Object[] args = new Object[]{t0};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, R> RayObject<R> call(RayFunc2<T0, T1, R> f, T0 t0, T1 t1) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, R> RayObject<R> call(RayFunc2<T0, T1, R> f, T0 t0, RayObject<T1> t1) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, R> RayObject<R> call(RayFunc2<T0, T1, R> f, RayObject<T0> t0, T1 t1) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, R> RayObject<R> call(RayFunc2<T0, T1, R> f, RayObject<T0> t0, RayObject<T1> t1) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, R> RayObject<R> call(RayFunc2<T0, T1, R> f, T0 t0, T1 t1, CallOptions options) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, R> RayObject<R> call(RayFunc2<T0, T1, R> f, T0 t0, RayObject<T1> t1, CallOptions options) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, R> RayObject<R> call(RayFunc2<T0, T1, R> f, RayObject<T0> t0, T1 t1, CallOptions options) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, R> RayObject<R> call(RayFunc2<T0, T1, R> f, RayObject<T0> t0, RayObject<T1> t1, CallOptions options) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1> void call(RayFuncVoid2<T0, T1> f, T0 t0, T1 t1) {
    Object[] args = new Object[]{t0, t1};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1> void call(RayFuncVoid2<T0, T1> f, T0 t0, RayObject<T1> t1) {
    Object[] args = new Object[]{t0, t1};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1> void call(RayFuncVoid2<T0, T1> f, RayObject<T0> t0, T1 t1) {
    Object[] args = new Object[]{t0, t1};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1> void call(RayFuncVoid2<T0, T1> f, RayObject<T0> t0, RayObject<T1> t1) {
    Object[] args = new Object[]{t0, t1};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1> void call(RayFuncVoid2<T0, T1> f, T0 t0, T1 t1, CallOptions options) {
    Object[] args = new Object[]{t0, t1};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1> void call(RayFuncVoid2<T0, T1> f, T0 t0, RayObject<T1> t1, CallOptions options) {
    Object[] args = new Object[]{t0, t1};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1> void call(RayFuncVoid2<T0, T1> f, RayObject<T0> t0, T1 t1, CallOptions options) {
    Object[] args = new Object[]{t0, t1};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1> void call(RayFuncVoid2<T0, T1> f, RayObject<T0> t0, RayObject<T1> t1, CallOptions options) {
    Object[] args = new Object[]{t0, t1};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, R> RayObject<R> call(RayFunc3<T0, T1, T2, R> f, T0 t0, T1 t1, T2 t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, R> RayObject<R> call(RayFunc3<T0, T1, T2, R> f, T0 t0, T1 t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, R> RayObject<R> call(RayFunc3<T0, T1, T2, R> f, T0 t0, RayObject<T1> t1, T2 t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, R> RayObject<R> call(RayFunc3<T0, T1, T2, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, R> RayObject<R> call(RayFunc3<T0, T1, T2, R> f, RayObject<T0> t0, T1 t1, T2 t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, R> RayObject<R> call(RayFunc3<T0, T1, T2, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, R> RayObject<R> call(RayFunc3<T0, T1, T2, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, R> RayObject<R> call(RayFunc3<T0, T1, T2, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, R> RayObject<R> call(RayFunc3<T0, T1, T2, R> f, T0 t0, T1 t1, T2 t2, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, R> RayObject<R> call(RayFunc3<T0, T1, T2, R> f, T0 t0, T1 t1, RayObject<T2> t2, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, R> RayObject<R> call(RayFunc3<T0, T1, T2, R> f, T0 t0, RayObject<T1> t1, T2 t2, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, R> RayObject<R> call(RayFunc3<T0, T1, T2, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, R> RayObject<R> call(RayFunc3<T0, T1, T2, R> f, RayObject<T0> t0, T1 t1, T2 t2, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, R> RayObject<R> call(RayFunc3<T0, T1, T2, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, R> RayObject<R> call(RayFunc3<T0, T1, T2, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, R> RayObject<R> call(RayFunc3<T0, T1, T2, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2> void call(RayFuncVoid3<T0, T1, T2> f, T0 t0, T1 t1, T2 t2) {
    Object[] args = new Object[]{t0, t1, t2};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2> void call(RayFuncVoid3<T0, T1, T2> f, T0 t0, T1 t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t0, t1, t2};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2> void call(RayFuncVoid3<T0, T1, T2> f, T0 t0, RayObject<T1> t1, T2 t2) {
    Object[] args = new Object[]{t0, t1, t2};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2> void call(RayFuncVoid3<T0, T1, T2> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t0, t1, t2};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2> void call(RayFuncVoid3<T0, T1, T2> f, RayObject<T0> t0, T1 t1, T2 t2) {
    Object[] args = new Object[]{t0, t1, t2};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2> void call(RayFuncVoid3<T0, T1, T2> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t0, t1, t2};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2> void call(RayFuncVoid3<T0, T1, T2> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2) {
    Object[] args = new Object[]{t0, t1, t2};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2> void call(RayFuncVoid3<T0, T1, T2> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t0, t1, t2};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2> void call(RayFuncVoid3<T0, T1, T2> f, T0 t0, T1 t1, T2 t2, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2> void call(RayFuncVoid3<T0, T1, T2> f, T0 t0, T1 t1, RayObject<T2> t2, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2> void call(RayFuncVoid3<T0, T1, T2> f, T0 t0, RayObject<T1> t1, T2 t2, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2> void call(RayFuncVoid3<T0, T1, T2> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2> void call(RayFuncVoid3<T0, T1, T2> f, RayObject<T0> t0, T1 t1, T2 t2, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2> void call(RayFuncVoid3<T0, T1, T2> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2> void call(RayFuncVoid3<T0, T1, T2> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2> void call(RayFuncVoid3<T0, T1, T2> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, T0 t0, T1 t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, T0 t0, T1 t1, T2 t2, T3 t3, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, R> RayObject<R> call(RayFunc4<T0, T1, T2, T3, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3> void call(RayFuncVoid4<T0, T1, T2, T3> f, T0 t0, T1 t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3> void call(RayFuncVoid4<T0, T1, T2, T3> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3> void call(RayFuncVoid4<T0, T1, T2, T3> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3> void call(RayFuncVoid4<T0, T1, T2, T3> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3> void call(RayFuncVoid4<T0, T1, T2, T3> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3> void call(RayFuncVoid4<T0, T1, T2, T3> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3> void call(RayFuncVoid4<T0, T1, T2, T3> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3> void call(RayFuncVoid4<T0, T1, T2, T3> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3> void call(RayFuncVoid4<T0, T1, T2, T3> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3> void call(RayFuncVoid4<T0, T1, T2, T3> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3> void call(RayFuncVoid4<T0, T1, T2, T3> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3> void call(RayFuncVoid4<T0, T1, T2, T3> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3> void call(RayFuncVoid4<T0, T1, T2, T3> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3> void call(RayFuncVoid4<T0, T1, T2, T3> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3> void call(RayFuncVoid4<T0, T1, T2, T3> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3> void call(RayFuncVoid4<T0, T1, T2, T3> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3> void call(RayFuncVoid4<T0, T1, T2, T3> f, T0 t0, T1 t1, T2 t2, T3 t3, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3> void call(RayFuncVoid4<T0, T1, T2, T3> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3> void call(RayFuncVoid4<T0, T1, T2, T3> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3> void call(RayFuncVoid4<T0, T1, T2, T3> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3> void call(RayFuncVoid4<T0, T1, T2, T3> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3> void call(RayFuncVoid4<T0, T1, T2, T3> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3> void call(RayFuncVoid4<T0, T1, T2, T3> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3> void call(RayFuncVoid4<T0, T1, T2, T3> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3> void call(RayFuncVoid4<T0, T1, T2, T3> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3> void call(RayFuncVoid4<T0, T1, T2, T3> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3> void call(RayFuncVoid4<T0, T1, T2, T3> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3> void call(RayFuncVoid4<T0, T1, T2, T3> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3> void call(RayFuncVoid4<T0, T1, T2, T3> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3> void call(RayFuncVoid4<T0, T1, T2, T3> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3> void call(RayFuncVoid4<T0, T1, T2, T3> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3> void call(RayFuncVoid4<T0, T1, T2, T3> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc5<T0, T1, T2, T3, T4, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4> void call(RayFuncVoid5<T0, T1, T2, T3, T4> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, R> RayObject<R> call(RayFunc6<T0, T1, T2, T3, T4, T5, R> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5> void call(RayFuncVoid6<T0, T1, T2, T3, T4, T5> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, CallOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    Ray.internal().call(f, args, options);
  }
  // ===========================================
  // Methods for remote actor method invocation.
  // ===========================================
  public static <A, R> RayObject<R> call(RayFunc1<A, R> f, RayActor<A> actor) {
    Object[] args = new Object[]{};
    return Ray.internal().call(f, actor, args);
  }
  public static <A> void call(RayFuncVoid1<A> f, RayActor<A> actor) {
    Object[] args = new Object[]{};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, R> RayObject<R> call(RayFunc2<A, T0, R> f, RayActor<A> actor, T0 t0) {
    Object[] args = new Object[]{t0};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, R> RayObject<R> call(RayFunc2<A, T0, R> f, RayActor<A> actor, RayObject<T0> t0) {
    Object[] args = new Object[]{t0};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0> void call(RayFuncVoid2<A, T0> f, RayActor<A> actor, T0 t0) {
    Object[] args = new Object[]{t0};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0> void call(RayFuncVoid2<A, T0> f, RayActor<A> actor, RayObject<T0> t0) {
    Object[] args = new Object[]{t0};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, R> RayObject<R> call(RayFunc3<A, T0, T1, R> f, RayActor<A> actor, T0 t0, T1 t1) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, R> RayObject<R> call(RayFunc3<A, T0, T1, R> f, RayActor<A> actor, T0 t0, RayObject<T1> t1) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, R> RayObject<R> call(RayFunc3<A, T0, T1, R> f, RayActor<A> actor, RayObject<T0> t0, T1 t1) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, R> RayObject<R> call(RayFunc3<A, T0, T1, R> f, RayActor<A> actor, RayObject<T0> t0, RayObject<T1> t1) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1> void call(RayFuncVoid3<A, T0, T1> f, RayActor<A> actor, T0 t0, T1 t1) {
    Object[] args = new Object[]{t0, t1};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1> void call(RayFuncVoid3<A, T0, T1> f, RayActor<A> actor, T0 t0, RayObject<T1> t1) {
    Object[] args = new Object[]{t0, t1};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1> void call(RayFuncVoid3<A, T0, T1> f, RayActor<A> actor, RayObject<T0> t0, T1 t1) {
    Object[] args = new Object[]{t0, t1};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1> void call(RayFuncVoid3<A, T0, T1> f, RayActor<A> actor, RayObject<T0> t0, RayObject<T1> t1) {
    Object[] args = new Object[]{t0, t1};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, R> RayObject<R> call(RayFunc4<A, T0, T1, T2, R> f, RayActor<A> actor, T0 t0, T1 t1, T2 t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, R> RayObject<R> call(RayFunc4<A, T0, T1, T2, R> f, RayActor<A> actor, T0 t0, T1 t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, R> RayObject<R> call(RayFunc4<A, T0, T1, T2, R> f, RayActor<A> actor, T0 t0, RayObject<T1> t1, T2 t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, R> RayObject<R> call(RayFunc4<A, T0, T1, T2, R> f, RayActor<A> actor, T0 t0, RayObject<T1> t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, R> RayObject<R> call(RayFunc4<A, T0, T1, T2, R> f, RayActor<A> actor, RayObject<T0> t0, T1 t1, T2 t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, R> RayObject<R> call(RayFunc4<A, T0, T1, T2, R> f, RayActor<A> actor, RayObject<T0> t0, T1 t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, R> RayObject<R> call(RayFunc4<A, T0, T1, T2, R> f, RayActor<A> actor, RayObject<T0> t0, RayObject<T1> t1, T2 t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, R> RayObject<R> call(RayFunc4<A, T0, T1, T2, R> f, RayActor<A> actor, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2> void call(RayFuncVoid4<A, T0, T1, T2> f, RayActor<A> actor, T0 t0, T1 t1, T2 t2) {
    Object[] args = new Object[]{t0, t1, t2};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2> void call(RayFuncVoid4<A, T0, T1, T2> f, RayActor<A> actor, T0 t0, T1 t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t0, t1, t2};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2> void call(RayFuncVoid4<A, T0, T1, T2> f, RayActor<A> actor, T0 t0, RayObject<T1> t1, T2 t2) {
    Object[] args = new Object[]{t0, t1, t2};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2> void call(RayFuncVoid4<A, T0, T1, T2> f, RayActor<A> actor, T0 t0, RayObject<T1> t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t0, t1, t2};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2> void call(RayFuncVoid4<A, T0, T1, T2> f, RayActor<A> actor, RayObject<T0> t0, T1 t1, T2 t2) {
    Object[] args = new Object[]{t0, t1, t2};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2> void call(RayFuncVoid4<A, T0, T1, T2> f, RayActor<A> actor, RayObject<T0> t0, T1 t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t0, t1, t2};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2> void call(RayFuncVoid4<A, T0, T1, T2> f, RayActor<A> actor, RayObject<T0> t0, RayObject<T1> t1, T2 t2) {
    Object[] args = new Object[]{t0, t1, t2};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2> void call(RayFuncVoid4<A, T0, T1, T2> f, RayActor<A> actor, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t0, t1, t2};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, R> RayObject<R> call(RayFunc5<A, T0, T1, T2, T3, R> f, RayActor<A> actor, T0 t0, T1 t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, R> RayObject<R> call(RayFunc5<A, T0, T1, T2, T3, R> f, RayActor<A> actor, T0 t0, T1 t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, R> RayObject<R> call(RayFunc5<A, T0, T1, T2, T3, R> f, RayActor<A> actor, T0 t0, T1 t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, R> RayObject<R> call(RayFunc5<A, T0, T1, T2, T3, R> f, RayActor<A> actor, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, R> RayObject<R> call(RayFunc5<A, T0, T1, T2, T3, R> f, RayActor<A> actor, T0 t0, RayObject<T1> t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, R> RayObject<R> call(RayFunc5<A, T0, T1, T2, T3, R> f, RayActor<A> actor, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, R> RayObject<R> call(RayFunc5<A, T0, T1, T2, T3, R> f, RayActor<A> actor, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, R> RayObject<R> call(RayFunc5<A, T0, T1, T2, T3, R> f, RayActor<A> actor, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, R> RayObject<R> call(RayFunc5<A, T0, T1, T2, T3, R> f, RayActor<A> actor, RayObject<T0> t0, T1 t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, R> RayObject<R> call(RayFunc5<A, T0, T1, T2, T3, R> f, RayActor<A> actor, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, R> RayObject<R> call(RayFunc5<A, T0, T1, T2, T3, R> f, RayActor<A> actor, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, R> RayObject<R> call(RayFunc5<A, T0, T1, T2, T3, R> f, RayActor<A> actor, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, R> RayObject<R> call(RayFunc5<A, T0, T1, T2, T3, R> f, RayActor<A> actor, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, R> RayObject<R> call(RayFunc5<A, T0, T1, T2, T3, R> f, RayActor<A> actor, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, R> RayObject<R> call(RayFunc5<A, T0, T1, T2, T3, R> f, RayActor<A> actor, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, R> RayObject<R> call(RayFunc5<A, T0, T1, T2, T3, R> f, RayActor<A> actor, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3> void call(RayFuncVoid5<A, T0, T1, T2, T3> f, RayActor<A> actor, T0 t0, T1 t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3> void call(RayFuncVoid5<A, T0, T1, T2, T3> f, RayActor<A> actor, T0 t0, T1 t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3> void call(RayFuncVoid5<A, T0, T1, T2, T3> f, RayActor<A> actor, T0 t0, T1 t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3> void call(RayFuncVoid5<A, T0, T1, T2, T3> f, RayActor<A> actor, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3> void call(RayFuncVoid5<A, T0, T1, T2, T3> f, RayActor<A> actor, T0 t0, RayObject<T1> t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3> void call(RayFuncVoid5<A, T0, T1, T2, T3> f, RayActor<A> actor, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3> void call(RayFuncVoid5<A, T0, T1, T2, T3> f, RayActor<A> actor, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3> void call(RayFuncVoid5<A, T0, T1, T2, T3> f, RayActor<A> actor, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3> void call(RayFuncVoid5<A, T0, T1, T2, T3> f, RayActor<A> actor, RayObject<T0> t0, T1 t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3> void call(RayFuncVoid5<A, T0, T1, T2, T3> f, RayActor<A> actor, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3> void call(RayFuncVoid5<A, T0, T1, T2, T3> f, RayActor<A> actor, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3> void call(RayFuncVoid5<A, T0, T1, T2, T3> f, RayActor<A> actor, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3> void call(RayFuncVoid5<A, T0, T1, T2, T3> f, RayActor<A> actor, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3> void call(RayFuncVoid5<A, T0, T1, T2, T3> f, RayActor<A> actor, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3> void call(RayFuncVoid5<A, T0, T1, T2, T3> f, RayActor<A> actor, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3> void call(RayFuncVoid5<A, T0, T1, T2, T3> f, RayActor<A> actor, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayActor<A> actor, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayActor<A> actor, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayActor<A> actor, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayActor<A> actor, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayActor<A> actor, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayActor<A> actor, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayActor<A> actor, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayActor<A> actor, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayActor<A> actor, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayActor<A> actor, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayActor<A> actor, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayActor<A> actor, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayActor<A> actor, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayActor<A> actor, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayActor<A> actor, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayActor<A> actor, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayActor<A> actor, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayActor<A> actor, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayActor<A> actor, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayActor<A> actor, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayActor<A> actor, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayActor<A> actor, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayActor<A> actor, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayActor<A> actor, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayActor<A> actor, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayActor<A> actor, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayActor<A> actor, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayActor<A> actor, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayActor<A> actor, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayActor<A> actor, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayActor<A> actor, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4, R> RayObject<R> call(RayFunc6<A, T0, T1, T2, T3, T4, R> f, RayActor<A> actor, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayActor<A> actor, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayActor<A> actor, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayActor<A> actor, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayActor<A> actor, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayActor<A> actor, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayActor<A> actor, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayActor<A> actor, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayActor<A> actor, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayActor<A> actor, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayActor<A> actor, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayActor<A> actor, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayActor<A> actor, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayActor<A> actor, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayActor<A> actor, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayActor<A> actor, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayActor<A> actor, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayActor<A> actor, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayActor<A> actor, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayActor<A> actor, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayActor<A> actor, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayActor<A> actor, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayActor<A> actor, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayActor<A> actor, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayActor<A> actor, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayActor<A> actor, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayActor<A> actor, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayActor<A> actor, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayActor<A> actor, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayActor<A> actor, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayActor<A> actor, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayActor<A> actor, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, actor, args);
  }
  public static <A, T0, T1, T2, T3, T4> void call(RayFuncVoid6<A, T0, T1, T2, T3, T4> f, RayActor<A> actor, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    Ray.internal().call(f, actor, args);
  }
  // ===========================
  // Methods for actor creation.
  // ===========================
  public static <A> RayActor<A> createActor(RayFunc0<A> f) {
    Object[] args = new Object[]{};
    return Ray.internal().createActor(f, args, null);
  }
  public static <A> RayActor<A> createActor(RayFunc0<A> f, ActorCreationOptions options) {
    Object[] args = new Object[]{};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, A> RayActor<A> createActor(RayFunc1<T0, A> f, T0 t0) {
    Object[] args = new Object[]{t0};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, A> RayActor<A> createActor(RayFunc1<T0, A> f, RayObject<T0> t0) {
    Object[] args = new Object[]{t0};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, A> RayActor<A> createActor(RayFunc1<T0, A> f, T0 t0, ActorCreationOptions options) {
    Object[] args = new Object[]{t0};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, A> RayActor<A> createActor(RayFunc1<T0, A> f, RayObject<T0> t0, ActorCreationOptions options) {
    Object[] args = new Object[]{t0};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, A> RayActor<A> createActor(RayFunc2<T0, T1, A> f, T0 t0, T1 t1) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, A> RayActor<A> createActor(RayFunc2<T0, T1, A> f, T0 t0, RayObject<T1> t1) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, A> RayActor<A> createActor(RayFunc2<T0, T1, A> f, RayObject<T0> t0, T1 t1) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, A> RayActor<A> createActor(RayFunc2<T0, T1, A> f, RayObject<T0> t0, RayObject<T1> t1) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, A> RayActor<A> createActor(RayFunc2<T0, T1, A> f, T0 t0, T1 t1, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, A> RayActor<A> createActor(RayFunc2<T0, T1, A> f, T0 t0, RayObject<T1> t1, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, A> RayActor<A> createActor(RayFunc2<T0, T1, A> f, RayObject<T0> t0, T1 t1, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, A> RayActor<A> createActor(RayFunc2<T0, T1, A> f, RayObject<T0> t0, RayObject<T1> t1, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, A> RayActor<A> createActor(RayFunc3<T0, T1, T2, A> f, T0 t0, T1 t1, T2 t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, A> RayActor<A> createActor(RayFunc3<T0, T1, T2, A> f, T0 t0, T1 t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, A> RayActor<A> createActor(RayFunc3<T0, T1, T2, A> f, T0 t0, RayObject<T1> t1, T2 t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, A> RayActor<A> createActor(RayFunc3<T0, T1, T2, A> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, A> RayActor<A> createActor(RayFunc3<T0, T1, T2, A> f, RayObject<T0> t0, T1 t1, T2 t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, A> RayActor<A> createActor(RayFunc3<T0, T1, T2, A> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, A> RayActor<A> createActor(RayFunc3<T0, T1, T2, A> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, A> RayActor<A> createActor(RayFunc3<T0, T1, T2, A> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, A> RayActor<A> createActor(RayFunc3<T0, T1, T2, A> f, T0 t0, T1 t1, T2 t2, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, A> RayActor<A> createActor(RayFunc3<T0, T1, T2, A> f, T0 t0, T1 t1, RayObject<T2> t2, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, A> RayActor<A> createActor(RayFunc3<T0, T1, T2, A> f, T0 t0, RayObject<T1> t1, T2 t2, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, A> RayActor<A> createActor(RayFunc3<T0, T1, T2, A> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, A> RayActor<A> createActor(RayFunc3<T0, T1, T2, A> f, RayObject<T0> t0, T1 t1, T2 t2, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, A> RayActor<A> createActor(RayFunc3<T0, T1, T2, A> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, A> RayActor<A> createActor(RayFunc3<T0, T1, T2, A> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, A> RayActor<A> createActor(RayFunc3<T0, T1, T2, A> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, A> RayActor<A> createActor(RayFunc4<T0, T1, T2, T3, A> f, T0 t0, T1 t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, A> RayActor<A> createActor(RayFunc4<T0, T1, T2, T3, A> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, A> RayActor<A> createActor(RayFunc4<T0, T1, T2, T3, A> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, A> RayActor<A> createActor(RayFunc4<T0, T1, T2, T3, A> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, A> RayActor<A> createActor(RayFunc4<T0, T1, T2, T3, A> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, A> RayActor<A> createActor(RayFunc4<T0, T1, T2, T3, A> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, A> RayActor<A> createActor(RayFunc4<T0, T1, T2, T3, A> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, A> RayActor<A> createActor(RayFunc4<T0, T1, T2, T3, A> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, A> RayActor<A> createActor(RayFunc4<T0, T1, T2, T3, A> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, A> RayActor<A> createActor(RayFunc4<T0, T1, T2, T3, A> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, A> RayActor<A> createActor(RayFunc4<T0, T1, T2, T3, A> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, A> RayActor<A> createActor(RayFunc4<T0, T1, T2, T3, A> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, A> RayActor<A> createActor(RayFunc4<T0, T1, T2, T3, A> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, A> RayActor<A> createActor(RayFunc4<T0, T1, T2, T3, A> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, A> RayActor<A> createActor(RayFunc4<T0, T1, T2, T3, A> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, A> RayActor<A> createActor(RayFunc4<T0, T1, T2, T3, A> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, A> RayActor<A> createActor(RayFunc4<T0, T1, T2, T3, A> f, T0 t0, T1 t1, T2 t2, T3 t3, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, A> RayActor<A> createActor(RayFunc4<T0, T1, T2, T3, A> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, A> RayActor<A> createActor(RayFunc4<T0, T1, T2, T3, A> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, A> RayActor<A> createActor(RayFunc4<T0, T1, T2, T3, A> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, A> RayActor<A> createActor(RayFunc4<T0, T1, T2, T3, A> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, A> RayActor<A> createActor(RayFunc4<T0, T1, T2, T3, A> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, A> RayActor<A> createActor(RayFunc4<T0, T1, T2, T3, A> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, A> RayActor<A> createActor(RayFunc4<T0, T1, T2, T3, A> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, A> RayActor<A> createActor(RayFunc4<T0, T1, T2, T3, A> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, A> RayActor<A> createActor(RayFunc4<T0, T1, T2, T3, A> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, A> RayActor<A> createActor(RayFunc4<T0, T1, T2, T3, A> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, A> RayActor<A> createActor(RayFunc4<T0, T1, T2, T3, A> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, A> RayActor<A> createActor(RayFunc4<T0, T1, T2, T3, A> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, A> RayActor<A> createActor(RayFunc4<T0, T1, T2, T3, A> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, A> RayActor<A> createActor(RayFunc4<T0, T1, T2, T3, A> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, A> RayActor<A> createActor(RayFunc4<T0, T1, T2, T3, A> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, A> RayActor<A> createActor(RayFunc5<T0, T1, T2, T3, T4, A> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, null);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, T5 t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, T5 t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  public static <T0, T1, T2, T3, T4, T5, A> RayActor<A> createActor(RayFunc6<T0, T1, T2, T3, T4, T5, A> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5, ActorCreationOptions options) {
    Object[] args = new Object[]{t0, t1, t2, t3, t4, t5};
    return Ray.internal().createActor(f, args, options);
  }
  // ===========================
  // Cross-language methods.
  // ===========================
  public static RayObject callPy(String moduleName, String functionName) {
    Object[] args = new Object[]{};
    return Ray.internal().callPy(moduleName, functionName, args, null);
  }
  public static RayObject callPy(String moduleName, String functionName, CallOptions options) {
    Object[] args = new Object[]{};
    return Ray.internal().callPy(moduleName, functionName, args, options);
  }
  public static RayObject callPy(String moduleName, String functionName, Object obj0) {
    Object[] args = new Object[]{obj0};
    return Ray.internal().callPy(moduleName, functionName, args, null);
  }
  public static RayObject callPy(String moduleName, String functionName, Object obj0, CallOptions options) {
    Object[] args = new Object[]{obj0};
    return Ray.internal().callPy(moduleName, functionName, args, options);
  }
  public static RayObject callPy(String moduleName, String functionName, Object obj0, Object obj1) {
    Object[] args = new Object[]{obj0, obj1};
    return Ray.internal().callPy(moduleName, functionName, args, null);
  }
  public static RayObject callPy(String moduleName, String functionName, Object obj0, Object obj1, CallOptions options) {
    Object[] args = new Object[]{obj0, obj1};
    return Ray.internal().callPy(moduleName, functionName, args, options);
  }
  public static RayObject callPy(String moduleName, String functionName, Object obj0, Object obj1, Object obj2) {
    Object[] args = new Object[]{obj0, obj1, obj2};
    return Ray.internal().callPy(moduleName, functionName, args, null);
  }
  public static RayObject callPy(String moduleName, String functionName, Object obj0, Object obj1, Object obj2, CallOptions options) {
    Object[] args = new Object[]{obj0, obj1, obj2};
    return Ray.internal().callPy(moduleName, functionName, args, options);
  }
  public static RayObject callPy(String moduleName, String functionName, Object obj0, Object obj1, Object obj2, Object obj3) {
    Object[] args = new Object[]{obj0, obj1, obj2, obj3};
    return Ray.internal().callPy(moduleName, functionName, args, null);
  }
  public static RayObject callPy(String moduleName, String functionName, Object obj0, Object obj1, Object obj2, Object obj3, CallOptions options) {
    Object[] args = new Object[]{obj0, obj1, obj2, obj3};
    return Ray.internal().callPy(moduleName, functionName, args, options);
  }
  public static RayObject callPy(String moduleName, String functionName, Object obj0, Object obj1, Object obj2, Object obj3, Object obj4) {
    Object[] args = new Object[]{obj0, obj1, obj2, obj3, obj4};
    return Ray.internal().callPy(moduleName, functionName, args, null);
  }
  public static RayObject callPy(String moduleName, String functionName, Object obj0, Object obj1, Object obj2, Object obj3, Object obj4, CallOptions options) {
    Object[] args = new Object[]{obj0, obj1, obj2, obj3, obj4};
    return Ray.internal().callPy(moduleName, functionName, args, options);
  }
  public static RayObject callPy(String moduleName, String functionName, Object obj0, Object obj1, Object obj2, Object obj3, Object obj4, Object obj5) {
    Object[] args = new Object[]{obj0, obj1, obj2, obj3, obj4, obj5};
    return Ray.internal().callPy(moduleName, functionName, args, null);
  }
  public static RayObject callPy(String moduleName, String functionName, Object obj0, Object obj1, Object obj2, Object obj3, Object obj4, Object obj5, CallOptions options) {
    Object[] args = new Object[]{obj0, obj1, obj2, obj3, obj4, obj5};
    return Ray.internal().callPy(moduleName, functionName, args, options);
  }
  public static RayObject callPy(RayPyActor pyActor, String functionName) {
    Object[] args = new Object[]{};
    return Ray.internal().callPy(pyActor, functionName, args);
  }
  public static RayObject callPy(RayPyActor pyActor, String functionName, Object obj0) {
    Object[] args = new Object[]{obj0};
    return Ray.internal().callPy(pyActor, functionName, args);
  }
  public static RayObject callPy(RayPyActor pyActor, String functionName, Object obj0, Object obj1) {
    Object[] args = new Object[]{obj0, obj1};
    return Ray.internal().callPy(pyActor, functionName, args);
  }
  public static RayObject callPy(RayPyActor pyActor, String functionName, Object obj0, Object obj1, Object obj2) {
    Object[] args = new Object[]{obj0, obj1, obj2};
    return Ray.internal().callPy(pyActor, functionName, args);
  }
  public static RayObject callPy(RayPyActor pyActor, String functionName, Object obj0, Object obj1, Object obj2, Object obj3) {
    Object[] args = new Object[]{obj0, obj1, obj2, obj3};
    return Ray.internal().callPy(pyActor, functionName, args);
  }
  public static RayObject callPy(RayPyActor pyActor, String functionName, Object obj0, Object obj1, Object obj2, Object obj3, Object obj4) {
    Object[] args = new Object[]{obj0, obj1, obj2, obj3, obj4};
    return Ray.internal().callPy(pyActor, functionName, args);
  }
  public static RayPyActor createPyActor(String moduleName, String className) {
    Object[] args = new Object[]{};
    return Ray.internal().createPyActor(moduleName, className, args, null);
  }
  public static RayPyActor createPyActor(String moduleName, String className, ActorCreationOptions options) {
    Object[] args = new Object[]{};
    return Ray.internal().createPyActor(moduleName, className, args, options);
  }
  public static RayPyActor createPyActor(String moduleName, String className, Object obj0) {
    Object[] args = new Object[]{obj0};
    return Ray.internal().createPyActor(moduleName, className, args, null);
  }
  public static RayPyActor createPyActor(String moduleName, String className, Object obj0, ActorCreationOptions options) {
    Object[] args = new Object[]{obj0};
    return Ray.internal().createPyActor(moduleName, className, args, options);
  }
  public static RayPyActor createPyActor(String moduleName, String className, Object obj0, Object obj1) {
    Object[] args = new Object[]{obj0, obj1};
    return Ray.internal().createPyActor(moduleName, className, args, null);
  }
  public static RayPyActor createPyActor(String moduleName, String className, Object obj0, Object obj1, ActorCreationOptions options) {
    Object[] args = new Object[]{obj0, obj1};
    return Ray.internal().createPyActor(moduleName, className, args, options);
  }
  public static RayPyActor createPyActor(String moduleName, String className, Object obj0, Object obj1, Object obj2) {
    Object[] args = new Object[]{obj0, obj1, obj2};
    return Ray.internal().createPyActor(moduleName, className, args, null);
  }
  public static RayPyActor createPyActor(String moduleName, String className, Object obj0, Object obj1, Object obj2, ActorCreationOptions options) {
    Object[] args = new Object[]{obj0, obj1, obj2};
    return Ray.internal().createPyActor(moduleName, className, args, options);
  }
  public static RayPyActor createPyActor(String moduleName, String className, Object obj0, Object obj1, Object obj2, Object obj3) {
    Object[] args = new Object[]{obj0, obj1, obj2, obj3};
    return Ray.internal().createPyActor(moduleName, className, args, null);
  }
  public static RayPyActor createPyActor(String moduleName, String className, Object obj0, Object obj1, Object obj2, Object obj3, ActorCreationOptions options) {
    Object[] args = new Object[]{obj0, obj1, obj2, obj3};
    return Ray.internal().createPyActor(moduleName, className, args, options);
  }
  public static RayPyActor createPyActor(String moduleName, String className, Object obj0, Object obj1, Object obj2, Object obj3, Object obj4) {
    Object[] args = new Object[]{obj0, obj1, obj2, obj3, obj4};
    return Ray.internal().createPyActor(moduleName, className, args, null);
  }
  public static RayPyActor createPyActor(String moduleName, String className, Object obj0, Object obj1, Object obj2, Object obj3, Object obj4, ActorCreationOptions options) {
    Object[] args = new Object[]{obj0, obj1, obj2, obj3, obj4};
    return Ray.internal().createPyActor(moduleName, className, args, options);
  }
  public static RayPyActor createPyActor(String moduleName, String className, Object obj0, Object obj1, Object obj2, Object obj3, Object obj4, Object obj5) {
    Object[] args = new Object[]{obj0, obj1, obj2, obj3, obj4, obj5};
    return Ray.internal().createPyActor(moduleName, className, args, null);
  }
  public static RayPyActor createPyActor(String moduleName, String className, Object obj0, Object obj1, Object obj2, Object obj3, Object obj4, Object obj5, ActorCreationOptions options) {
    Object[] args = new Object[]{obj0, obj1, obj2, obj3, obj4, obj5};
    return Ray.internal().createPyActor(moduleName, className, args, options);
  }
}
