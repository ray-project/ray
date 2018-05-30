package org.ray.api;

import java.util.Collection;
import org.ray.api.funcs.RayFunc_0_1;
import org.ray.api.funcs.RayFunc_0_2;
import org.ray.api.funcs.RayFunc_0_3;
import org.ray.api.funcs.RayFunc_0_4;
import org.ray.api.funcs.RayFunc_0_n;
import org.ray.api.funcs.RayFunc_0_n_list;
import org.ray.api.funcs.RayFunc_1_1;
import org.ray.api.funcs.RayFunc_1_2;
import org.ray.api.funcs.RayFunc_1_3;
import org.ray.api.funcs.RayFunc_1_4;
import org.ray.api.funcs.RayFunc_1_n;
import org.ray.api.funcs.RayFunc_1_n_list;
import org.ray.api.funcs.RayFunc_2_1;
import org.ray.api.funcs.RayFunc_2_2;
import org.ray.api.funcs.RayFunc_2_3;
import org.ray.api.funcs.RayFunc_2_4;
import org.ray.api.funcs.RayFunc_2_n;
import org.ray.api.funcs.RayFunc_2_n_list;
import org.ray.api.funcs.RayFunc_3_1;
import org.ray.api.funcs.RayFunc_3_2;
import org.ray.api.funcs.RayFunc_3_3;
import org.ray.api.funcs.RayFunc_3_4;
import org.ray.api.funcs.RayFunc_3_n;
import org.ray.api.funcs.RayFunc_3_n_list;
import org.ray.api.funcs.RayFunc_4_1;
import org.ray.api.funcs.RayFunc_4_2;
import org.ray.api.funcs.RayFunc_4_3;
import org.ray.api.funcs.RayFunc_4_4;
import org.ray.api.funcs.RayFunc_4_n;
import org.ray.api.funcs.RayFunc_4_n_list;
import org.ray.api.funcs.RayFunc_5_1;
import org.ray.api.funcs.RayFunc_5_2;
import org.ray.api.funcs.RayFunc_5_3;
import org.ray.api.funcs.RayFunc_5_4;
import org.ray.api.funcs.RayFunc_5_n;
import org.ray.api.funcs.RayFunc_5_n_list;
import org.ray.api.funcs.RayFunc_6_1;
import org.ray.api.funcs.RayFunc_6_2;
import org.ray.api.funcs.RayFunc_6_3;
import org.ray.api.funcs.RayFunc_6_4;
import org.ray.api.funcs.RayFunc_6_n;
import org.ray.api.funcs.RayFunc_6_n_list;
import org.ray.api.returns.RayObjects2;
import org.ray.api.returns.RayObjects3;
import org.ray.api.returns.RayObjects4;

@SuppressWarnings({"rawtypes", "unchecked"})
class Rpc {

  public static <R> RayList<R> call_n(RayFunc_0_n_list<R> f, Integer returnCount) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().callWithReturnIndices(null, RayFunc_0_n_list.class, f, returnCount);
    } else {
      return Ray.internal().callWithReturnIndices(null, () -> f.apply(), returnCount);
    }
  }

  public static <R, RID> RayMap<RID, R> call_n(RayFunc_0_n<R, RID> f, Collection<RID> returnids) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().callWithReturnLabels(null, RayFunc_0_n.class, f, returnids);
    } else {
      return Ray.internal().callWithReturnLabels(null, () -> f.apply(null), returnids);
    }
  }

  public static <R0> RayObject<R0> call(RayFunc_0_1<R0> f) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_0_1.class, f, 1).objs[0];
    } else {
      return Ray.internal().call(null, () -> f.apply(), 1).objs[0];
    }
  }

  public static <R0, R1> RayObjects2<R0, R1> call_2(RayFunc_0_2<R0, R1> f) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(Ray.internal().call(null, RayFunc_0_2.class, f, 2).objs);
    } else {
      return new RayObjects2(Ray.internal().call(null, () -> f.apply(), 2).objs);
    }
  }

  public static <R0, R1, R2> RayObjects3<R0, R1, R2> call_3(RayFunc_0_3<R0, R1, R2> f) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(Ray.internal().call(null, RayFunc_0_3.class, f, 3).objs);
    } else {
      return new RayObjects3(Ray.internal().call(null, () -> f.apply(), 3).objs);
    }
  }

  public static <R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(RayFunc_0_4<R0, R1, R2, R3> f) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(Ray.internal().call(null, RayFunc_0_4.class, f, 4).objs);
    } else {
      return new RayObjects4(Ray.internal().call(null, () -> f.apply(), 4).objs);
    }
  }

  public static <T0, R> RayList<R> call_n(RayFunc_1_n_list<T0, R> f, Integer returnCount, T0 t0) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_1_n_list.class, f, returnCount, t0);
    } else {
      return Ray.internal().callWithReturnIndices(null, () -> f.apply(null), returnCount, t0);
    }
  }

  public static <T0, R> RayList<R> call_n(RayFunc_1_n_list<T0, R> f, Integer returnCount,
      RayObject<T0> t0) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_1_n_list.class, f, returnCount, t0);
    } else {
      return Ray.internal().callWithReturnIndices(null, () -> f.apply(null), returnCount, t0);
    }
  }

  public static <T0, R, RID> RayMap<RID, R> call_n(RayFunc_1_n<T0, R, RID> f,
      Collection<RID> returnids, T0 t0) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().callWithReturnLabels(null, RayFunc_1_n.class, f, returnids, t0);
    } else {
      return Ray.internal().callWithReturnLabels(null, () -> f.apply(null, null), returnids, t0);
    }
  }

  public static <T0, R, RID> RayMap<RID, R> call_n(RayFunc_1_n<T0, R, RID> f,
      Collection<RID> returnids, RayObject<T0> t0) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().callWithReturnLabels(null, RayFunc_1_n.class, f, returnids, t0);
    } else {
      return Ray.internal().callWithReturnLabels(null, () -> f.apply(null, null), returnids, t0);
    }
  }

  public static <T0, R0> RayObject<R0> call(RayFunc_1_1<T0, R0> f, T0 t0) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_1_1.class, f, 1, t0).objs[0];
    } else {
      return Ray.internal().call(null, () -> f.apply(null), 1, t0).objs[0];
    }
  }

  public static <T0, R0> RayObject<R0> call(RayFunc_1_1<T0, R0> f, RayObject<T0> t0) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_1_1.class, f, 1, t0).objs[0];
    } else {
      return Ray.internal().call(null, () -> f.apply(null), 1, t0).objs[0];
    }
  }

  public static <T0, R0, R1> RayObjects2<R0, R1> call_2(RayFunc_1_2<T0, R0, R1> f, T0 t0) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(Ray.internal().call(null, RayFunc_1_2.class, f, 2, t0).objs);
    } else {
      return new RayObjects2(Ray.internal().call(null, () -> f.apply(null), 2, t0).objs);
    }
  }

  public static <T0, R0, R1> RayObjects2<R0, R1> call_2(RayFunc_1_2<T0, R0, R1> f,
      RayObject<T0> t0) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(Ray.internal().call(null, RayFunc_1_2.class, f, 2, t0).objs);
    } else {
      return new RayObjects2(Ray.internal().call(null, () -> f.apply(null), 2, t0).objs);
    }
  }

  public static <T0, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(RayFunc_1_3<T0, R0, R1, R2> f,
      T0 t0) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(Ray.internal().call(null, RayFunc_1_3.class, f, 3, t0).objs);
    } else {
      return new RayObjects3(Ray.internal().call(null, () -> f.apply(null), 3, t0).objs);
    }
  }

  public static <T0, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(RayFunc_1_3<T0, R0, R1, R2> f,
      RayObject<T0> t0) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(Ray.internal().call(null, RayFunc_1_3.class, f, 3, t0).objs);
    } else {
      return new RayObjects3(Ray.internal().call(null, () -> f.apply(null), 3, t0).objs);
    }
  }

  public static <T0, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_1_4<T0, R0, R1, R2, R3> f, T0 t0) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(Ray.internal().call(null, RayFunc_1_4.class, f, 4, t0).objs);
    } else {
      return new RayObjects4(Ray.internal().call(null, () -> f.apply(null), 4, t0).objs);
    }
  }

  public static <T0, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_1_4<T0, R0, R1, R2, R3> f, RayObject<T0> t0) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(Ray.internal().call(null, RayFunc_1_4.class, f, 4, t0).objs);
    } else {
      return new RayObjects4(Ray.internal().call(null, () -> f.apply(null), 4, t0).objs);
    }
  }

  public static <T0, T1, R> RayList<R> call_n(RayFunc_2_n_list<T0, T1, R> f, Integer returnCount,
      T0 t0, T1 t1) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_2_n_list.class, f, returnCount, t0, t1);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null), returnCount, t0, t1);
    }
  }

  public static <T0, T1, R> RayList<R> call_n(RayFunc_2_n_list<T0, T1, R> f, Integer returnCount,
      RayObject<T0> t0, T1 t1) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_2_n_list.class, f, returnCount, t0, t1);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null), returnCount, t0, t1);
    }
  }

  public static <T0, T1, R> RayList<R> call_n(RayFunc_2_n_list<T0, T1, R> f, Integer returnCount,
      T0 t0, RayObject<T1> t1) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_2_n_list.class, f, returnCount, t0, t1);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null), returnCount, t0, t1);
    }
  }

  public static <T0, T1, R> RayList<R> call_n(RayFunc_2_n_list<T0, T1, R> f, Integer returnCount,
      RayObject<T0> t0, RayObject<T1> t1) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_2_n_list.class, f, returnCount, t0, t1);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null), returnCount, t0, t1);
    }
  }

  public static <T0, T1, R, RID> RayMap<RID, R> call_n(RayFunc_2_n<T0, T1, R, RID> f,
      Collection<RID> returnids, T0 t0, T1 t1) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().callWithReturnLabels(null, RayFunc_2_n.class, f, returnids, t0, t1);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null), returnids, t0, t1);
    }
  }

  public static <T0, T1, R, RID> RayMap<RID, R> call_n(RayFunc_2_n<T0, T1, R, RID> f,
      Collection<RID> returnids, RayObject<T0> t0, T1 t1) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().callWithReturnLabels(null, RayFunc_2_n.class, f, returnids, t0, t1);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null), returnids, t0, t1);
    }
  }

  public static <T0, T1, R, RID> RayMap<RID, R> call_n(RayFunc_2_n<T0, T1, R, RID> f,
      Collection<RID> returnids, T0 t0, RayObject<T1> t1) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().callWithReturnLabels(null, RayFunc_2_n.class, f, returnids, t0, t1);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null), returnids, t0, t1);
    }
  }

  public static <T0, T1, R, RID> RayMap<RID, R> call_n(RayFunc_2_n<T0, T1, R, RID> f,
      Collection<RID> returnids, RayObject<T0> t0, RayObject<T1> t1) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().callWithReturnLabels(null, RayFunc_2_n.class, f, returnids, t0, t1);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null), returnids, t0, t1);
    }
  }

  public static <T0, T1, R0> RayObject<R0> call(RayFunc_2_1<T0, T1, R0> f, T0 t0, T1 t1) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_2_1.class, f, 1, t0, t1).objs[0];
    } else {
      return Ray.internal().call(null, () -> f.apply(null, null), 1, t0, t1).objs[0];
    }
  }

  public static <T0, T1, R0> RayObject<R0> call(RayFunc_2_1<T0, T1, R0> f, RayObject<T0> t0,
      T1 t1) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_2_1.class, f, 1, t0, t1).objs[0];
    } else {
      return Ray.internal().call(null, () -> f.apply(null, null), 1, t0, t1).objs[0];
    }
  }

  public static <T0, T1, R0> RayObject<R0> call(RayFunc_2_1<T0, T1, R0> f, T0 t0,
      RayObject<T1> t1) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_2_1.class, f, 1, t0, t1).objs[0];
    } else {
      return Ray.internal().call(null, () -> f.apply(null, null), 1, t0, t1).objs[0];
    }
  }

  public static <T0, T1, R0> RayObject<R0> call(RayFunc_2_1<T0, T1, R0> f, RayObject<T0> t0,
      RayObject<T1> t1) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_2_1.class, f, 1, t0, t1).objs[0];
    } else {
      return Ray.internal().call(null, () -> f.apply(null, null), 1, t0, t1).objs[0];
    }
  }

  public static <T0, T1, R0, R1> RayObjects2<R0, R1> call_2(RayFunc_2_2<T0, T1, R0, R1> f, T0 t0,
      T1 t1) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(Ray.internal().call(null, RayFunc_2_2.class, f, 2, t0, t1).objs);
    } else {
      return new RayObjects2(Ray.internal().call(null, () -> f.apply(null, null), 2, t0, t1).objs);
    }
  }

  public static <T0, T1, R0, R1> RayObjects2<R0, R1> call_2(RayFunc_2_2<T0, T1, R0, R1> f,
      RayObject<T0> t0, T1 t1) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(Ray.internal().call(null, RayFunc_2_2.class, f, 2, t0, t1).objs);
    } else {
      return new RayObjects2(Ray.internal().call(null, () -> f.apply(null, null), 2, t0, t1).objs);
    }
  }

  public static <T0, T1, R0, R1> RayObjects2<R0, R1> call_2(RayFunc_2_2<T0, T1, R0, R1> f, T0 t0,
      RayObject<T1> t1) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(Ray.internal().call(null, RayFunc_2_2.class, f, 2, t0, t1).objs);
    } else {
      return new RayObjects2(Ray.internal().call(null, () -> f.apply(null, null), 2, t0, t1).objs);
    }
  }

  public static <T0, T1, R0, R1> RayObjects2<R0, R1> call_2(RayFunc_2_2<T0, T1, R0, R1> f,
      RayObject<T0> t0, RayObject<T1> t1) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(Ray.internal().call(null, RayFunc_2_2.class, f, 2, t0, t1).objs);
    } else {
      return new RayObjects2(Ray.internal().call(null, () -> f.apply(null, null), 2, t0, t1).objs);
    }
  }

  public static <T0, T1, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_2_3<T0, T1, R0, R1, R2> f, T0 t0, T1 t1) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(Ray.internal().call(null, RayFunc_2_3.class, f, 3, t0, t1).objs);
    } else {
      return new RayObjects3(Ray.internal().call(null, () -> f.apply(null, null), 3, t0, t1).objs);
    }
  }

  public static <T0, T1, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_2_3<T0, T1, R0, R1, R2> f, RayObject<T0> t0, T1 t1) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(Ray.internal().call(null, RayFunc_2_3.class, f, 3, t0, t1).objs);
    } else {
      return new RayObjects3(Ray.internal().call(null, () -> f.apply(null, null), 3, t0, t1).objs);
    }
  }

  public static <T0, T1, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_2_3<T0, T1, R0, R1, R2> f, T0 t0, RayObject<T1> t1) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(Ray.internal().call(null, RayFunc_2_3.class, f, 3, t0, t1).objs);
    } else {
      return new RayObjects3(Ray.internal().call(null, () -> f.apply(null, null), 3, t0, t1).objs);
    }
  }

  public static <T0, T1, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_2_3<T0, T1, R0, R1, R2> f, RayObject<T0> t0, RayObject<T1> t1) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(Ray.internal().call(null, RayFunc_2_3.class, f, 3, t0, t1).objs);
    } else {
      return new RayObjects3(Ray.internal().call(null, () -> f.apply(null, null), 3, t0, t1).objs);
    }
  }

  public static <T0, T1, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_2_4<T0, T1, R0, R1, R2, R3> f, T0 t0, T1 t1) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(Ray.internal().call(null, RayFunc_2_4.class, f, 4, t0, t1).objs);
    } else {
      return new RayObjects4(Ray.internal().call(null, () -> f.apply(null, null), 4, t0, t1).objs);
    }
  }

  public static <T0, T1, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_2_4<T0, T1, R0, R1, R2, R3> f, RayObject<T0> t0, T1 t1) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(Ray.internal().call(null, RayFunc_2_4.class, f, 4, t0, t1).objs);
    } else {
      return new RayObjects4(Ray.internal().call(null, () -> f.apply(null, null), 4, t0, t1).objs);
    }
  }

  public static <T0, T1, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_2_4<T0, T1, R0, R1, R2, R3> f, T0 t0, RayObject<T1> t1) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(Ray.internal().call(null, RayFunc_2_4.class, f, 4, t0, t1).objs);
    } else {
      return new RayObjects4(Ray.internal().call(null, () -> f.apply(null, null), 4, t0, t1).objs);
    }
  }

  public static <T0, T1, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_2_4<T0, T1, R0, R1, R2, R3> f, RayObject<T0> t0, RayObject<T1> t1) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(Ray.internal().call(null, RayFunc_2_4.class, f, 4, t0, t1).objs);
    } else {
      return new RayObjects4(Ray.internal().call(null, () -> f.apply(null, null), 4, t0, t1).objs);
    }
  }

  public static <T0, T1, T2, R> RayList<R> call_n(RayFunc_3_n_list<T0, T1, T2, R> f,
      Integer returnCount, T0 t0, T1 t1, T2 t2) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_3_n_list.class, f, returnCount, t0, t1, t2);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null), returnCount, t0, t1, t2);
    }
  }

  public static <T0, T1, T2, R> RayList<R> call_n(RayFunc_3_n_list<T0, T1, T2, R> f,
      Integer returnCount, RayObject<T0> t0, T1 t1, T2 t2) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_3_n_list.class, f, returnCount, t0, t1, t2);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null), returnCount, t0, t1, t2);
    }
  }

  public static <T0, T1, T2, R> RayList<R> call_n(RayFunc_3_n_list<T0, T1, T2, R> f,
      Integer returnCount, T0 t0, RayObject<T1> t1, T2 t2) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_3_n_list.class, f, returnCount, t0, t1, t2);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null), returnCount, t0, t1, t2);
    }
  }

  public static <T0, T1, T2, R> RayList<R> call_n(RayFunc_3_n_list<T0, T1, T2, R> f,
      Integer returnCount, RayObject<T0> t0, RayObject<T1> t1, T2 t2) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_3_n_list.class, f, returnCount, t0, t1, t2);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null), returnCount, t0, t1, t2);
    }
  }

  public static <T0, T1, T2, R> RayList<R> call_n(RayFunc_3_n_list<T0, T1, T2, R> f,
      Integer returnCount, T0 t0, T1 t1, RayObject<T2> t2) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_3_n_list.class, f, returnCount, t0, t1, t2);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null), returnCount, t0, t1, t2);
    }
  }

  public static <T0, T1, T2, R> RayList<R> call_n(RayFunc_3_n_list<T0, T1, T2, R> f,
      Integer returnCount, RayObject<T0> t0, T1 t1, RayObject<T2> t2) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_3_n_list.class, f, returnCount, t0, t1, t2);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null), returnCount, t0, t1, t2);
    }
  }

  public static <T0, T1, T2, R> RayList<R> call_n(RayFunc_3_n_list<T0, T1, T2, R> f,
      Integer returnCount, T0 t0, RayObject<T1> t1, RayObject<T2> t2) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_3_n_list.class, f, returnCount, t0, t1, t2);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null), returnCount, t0, t1, t2);
    }
  }

  public static <T0, T1, T2, R> RayList<R> call_n(RayFunc_3_n_list<T0, T1, T2, R> f,
      Integer returnCount, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_3_n_list.class, f, returnCount, t0, t1, t2);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null), returnCount, t0, t1, t2);
    }
  }

  public static <T0, T1, T2, R, RID> RayMap<RID, R> call_n(RayFunc_3_n<T0, T1, T2, R, RID> f,
      Collection<RID> returnids, T0 t0, T1 t1, T2 t2) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_3_n.class, f, returnids, t0, t1, t2);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null), returnids, t0, t1,
              t2);
    }
  }

  public static <T0, T1, T2, R, RID> RayMap<RID, R> call_n(RayFunc_3_n<T0, T1, T2, R, RID> f,
      Collection<RID> returnids, RayObject<T0> t0, T1 t1, T2 t2) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_3_n.class, f, returnids, t0, t1, t2);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null), returnids, t0, t1,
              t2);
    }
  }

  public static <T0, T1, T2, R, RID> RayMap<RID, R> call_n(RayFunc_3_n<T0, T1, T2, R, RID> f,
      Collection<RID> returnids, T0 t0, RayObject<T1> t1, T2 t2) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_3_n.class, f, returnids, t0, t1, t2);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null), returnids, t0, t1,
              t2);
    }
  }

  public static <T0, T1, T2, R, RID> RayMap<RID, R> call_n(RayFunc_3_n<T0, T1, T2, R, RID> f,
      Collection<RID> returnids, RayObject<T0> t0, RayObject<T1> t1, T2 t2) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_3_n.class, f, returnids, t0, t1, t2);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null), returnids, t0, t1,
              t2);
    }
  }

  public static <T0, T1, T2, R, RID> RayMap<RID, R> call_n(RayFunc_3_n<T0, T1, T2, R, RID> f,
      Collection<RID> returnids, T0 t0, T1 t1, RayObject<T2> t2) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_3_n.class, f, returnids, t0, t1, t2);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null), returnids, t0, t1,
              t2);
    }
  }

  public static <T0, T1, T2, R, RID> RayMap<RID, R> call_n(RayFunc_3_n<T0, T1, T2, R, RID> f,
      Collection<RID> returnids, RayObject<T0> t0, T1 t1, RayObject<T2> t2) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_3_n.class, f, returnids, t0, t1, t2);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null), returnids, t0, t1,
              t2);
    }
  }

  public static <T0, T1, T2, R, RID> RayMap<RID, R> call_n(RayFunc_3_n<T0, T1, T2, R, RID> f,
      Collection<RID> returnids, T0 t0, RayObject<T1> t1, RayObject<T2> t2) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_3_n.class, f, returnids, t0, t1, t2);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null), returnids, t0, t1,
              t2);
    }
  }

  public static <T0, T1, T2, R, RID> RayMap<RID, R> call_n(RayFunc_3_n<T0, T1, T2, R, RID> f,
      Collection<RID> returnids, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_3_n.class, f, returnids, t0, t1, t2);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null), returnids, t0, t1,
              t2);
    }
  }

  public static <T0, T1, T2, R0> RayObject<R0> call(RayFunc_3_1<T0, T1, T2, R0> f, T0 t0, T1 t1,
      T2 t2) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_3_1.class, f, 1, t0, t1, t2).objs[0];
    } else {
      return Ray.internal().call(null, () -> f.apply(null, null, null), 1, t0, t1, t2).objs[0];
    }
  }

  public static <T0, T1, T2, R0> RayObject<R0> call(RayFunc_3_1<T0, T1, T2, R0> f, RayObject<T0> t0,
      T1 t1, T2 t2) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_3_1.class, f, 1, t0, t1, t2).objs[0];
    } else {
      return Ray.internal().call(null, () -> f.apply(null, null, null), 1, t0, t1, t2).objs[0];
    }
  }

  public static <T0, T1, T2, R0> RayObject<R0> call(RayFunc_3_1<T0, T1, T2, R0> f, T0 t0,
      RayObject<T1> t1, T2 t2) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_3_1.class, f, 1, t0, t1, t2).objs[0];
    } else {
      return Ray.internal().call(null, () -> f.apply(null, null, null), 1, t0, t1, t2).objs[0];
    }
  }

  public static <T0, T1, T2, R0> RayObject<R0> call(RayFunc_3_1<T0, T1, T2, R0> f, RayObject<T0> t0,
      RayObject<T1> t1, T2 t2) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_3_1.class, f, 1, t0, t1, t2).objs[0];
    } else {
      return Ray.internal().call(null, () -> f.apply(null, null, null), 1, t0, t1, t2).objs[0];
    }
  }

  public static <T0, T1, T2, R0> RayObject<R0> call(RayFunc_3_1<T0, T1, T2, R0> f, T0 t0, T1 t1,
      RayObject<T2> t2) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_3_1.class, f, 1, t0, t1, t2).objs[0];
    } else {
      return Ray.internal().call(null, () -> f.apply(null, null, null), 1, t0, t1, t2).objs[0];
    }
  }

  public static <T0, T1, T2, R0> RayObject<R0> call(RayFunc_3_1<T0, T1, T2, R0> f, RayObject<T0> t0,
      T1 t1, RayObject<T2> t2) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_3_1.class, f, 1, t0, t1, t2).objs[0];
    } else {
      return Ray.internal().call(null, () -> f.apply(null, null, null), 1, t0, t1, t2).objs[0];
    }
  }

  public static <T0, T1, T2, R0> RayObject<R0> call(RayFunc_3_1<T0, T1, T2, R0> f, T0 t0,
      RayObject<T1> t1, RayObject<T2> t2) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_3_1.class, f, 1, t0, t1, t2).objs[0];
    } else {
      return Ray.internal().call(null, () -> f.apply(null, null, null), 1, t0, t1, t2).objs[0];
    }
  }

  public static <T0, T1, T2, R0> RayObject<R0> call(RayFunc_3_1<T0, T1, T2, R0> f, RayObject<T0> t0,
      RayObject<T1> t1, RayObject<T2> t2) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_3_1.class, f, 1, t0, t1, t2).objs[0];
    } else {
      return Ray.internal().call(null, () -> f.apply(null, null, null), 1, t0, t1, t2).objs[0];
    }
  }

  public static <T0, T1, T2, R0, R1> RayObjects2<R0, R1> call_2(RayFunc_3_2<T0, T1, T2, R0, R1> f,
      T0 t0, T1 t1, T2 t2) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(Ray.internal().call(null, RayFunc_3_2.class, f, 2, t0, t1, t2).objs);
    } else {
      return new RayObjects2(
          Ray.internal().call(null, () -> f.apply(null, null, null), 2, t0, t1, t2).objs);
    }
  }

  public static <T0, T1, T2, R0, R1> RayObjects2<R0, R1> call_2(RayFunc_3_2<T0, T1, T2, R0, R1> f,
      RayObject<T0> t0, T1 t1, T2 t2) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(Ray.internal().call(null, RayFunc_3_2.class, f, 2, t0, t1, t2).objs);
    } else {
      return new RayObjects2(
          Ray.internal().call(null, () -> f.apply(null, null, null), 2, t0, t1, t2).objs);
    }
  }

  public static <T0, T1, T2, R0, R1> RayObjects2<R0, R1> call_2(RayFunc_3_2<T0, T1, T2, R0, R1> f,
      T0 t0, RayObject<T1> t1, T2 t2) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(Ray.internal().call(null, RayFunc_3_2.class, f, 2, t0, t1, t2).objs);
    } else {
      return new RayObjects2(
          Ray.internal().call(null, () -> f.apply(null, null, null), 2, t0, t1, t2).objs);
    }
  }

  public static <T0, T1, T2, R0, R1> RayObjects2<R0, R1> call_2(RayFunc_3_2<T0, T1, T2, R0, R1> f,
      RayObject<T0> t0, RayObject<T1> t1, T2 t2) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(Ray.internal().call(null, RayFunc_3_2.class, f, 2, t0, t1, t2).objs);
    } else {
      return new RayObjects2(
          Ray.internal().call(null, () -> f.apply(null, null, null), 2, t0, t1, t2).objs);
    }
  }

  public static <T0, T1, T2, R0, R1> RayObjects2<R0, R1> call_2(RayFunc_3_2<T0, T1, T2, R0, R1> f,
      T0 t0, T1 t1, RayObject<T2> t2) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(Ray.internal().call(null, RayFunc_3_2.class, f, 2, t0, t1, t2).objs);
    } else {
      return new RayObjects2(
          Ray.internal().call(null, () -> f.apply(null, null, null), 2, t0, t1, t2).objs);
    }
  }

  public static <T0, T1, T2, R0, R1> RayObjects2<R0, R1> call_2(RayFunc_3_2<T0, T1, T2, R0, R1> f,
      RayObject<T0> t0, T1 t1, RayObject<T2> t2) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(Ray.internal().call(null, RayFunc_3_2.class, f, 2, t0, t1, t2).objs);
    } else {
      return new RayObjects2(
          Ray.internal().call(null, () -> f.apply(null, null, null), 2, t0, t1, t2).objs);
    }
  }

  public static <T0, T1, T2, R0, R1> RayObjects2<R0, R1> call_2(RayFunc_3_2<T0, T1, T2, R0, R1> f,
      T0 t0, RayObject<T1> t1, RayObject<T2> t2) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(Ray.internal().call(null, RayFunc_3_2.class, f, 2, t0, t1, t2).objs);
    } else {
      return new RayObjects2(
          Ray.internal().call(null, () -> f.apply(null, null, null), 2, t0, t1, t2).objs);
    }
  }

  public static <T0, T1, T2, R0, R1> RayObjects2<R0, R1> call_2(RayFunc_3_2<T0, T1, T2, R0, R1> f,
      RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(Ray.internal().call(null, RayFunc_3_2.class, f, 2, t0, t1, t2).objs);
    } else {
      return new RayObjects2(
          Ray.internal().call(null, () -> f.apply(null, null, null), 2, t0, t1, t2).objs);
    }
  }

  public static <T0, T1, T2, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_3_3<T0, T1, T2, R0, R1, R2> f, T0 t0, T1 t1, T2 t2) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(Ray.internal().call(null, RayFunc_3_3.class, f, 3, t0, t1, t2).objs);
    } else {
      return new RayObjects3(
          Ray.internal().call(null, () -> f.apply(null, null, null), 3, t0, t1, t2).objs);
    }
  }

  public static <T0, T1, T2, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_3_3<T0, T1, T2, R0, R1, R2> f, RayObject<T0> t0, T1 t1, T2 t2) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(Ray.internal().call(null, RayFunc_3_3.class, f, 3, t0, t1, t2).objs);
    } else {
      return new RayObjects3(
          Ray.internal().call(null, () -> f.apply(null, null, null), 3, t0, t1, t2).objs);
    }
  }

  public static <T0, T1, T2, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_3_3<T0, T1, T2, R0, R1, R2> f, T0 t0, RayObject<T1> t1, T2 t2) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(Ray.internal().call(null, RayFunc_3_3.class, f, 3, t0, t1, t2).objs);
    } else {
      return new RayObjects3(
          Ray.internal().call(null, () -> f.apply(null, null, null), 3, t0, t1, t2).objs);
    }
  }

  public static <T0, T1, T2, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_3_3<T0, T1, T2, R0, R1, R2> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(Ray.internal().call(null, RayFunc_3_3.class, f, 3, t0, t1, t2).objs);
    } else {
      return new RayObjects3(
          Ray.internal().call(null, () -> f.apply(null, null, null), 3, t0, t1, t2).objs);
    }
  }

  public static <T0, T1, T2, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_3_3<T0, T1, T2, R0, R1, R2> f, T0 t0, T1 t1, RayObject<T2> t2) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(Ray.internal().call(null, RayFunc_3_3.class, f, 3, t0, t1, t2).objs);
    } else {
      return new RayObjects3(
          Ray.internal().call(null, () -> f.apply(null, null, null), 3, t0, t1, t2).objs);
    }
  }

  public static <T0, T1, T2, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_3_3<T0, T1, T2, R0, R1, R2> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(Ray.internal().call(null, RayFunc_3_3.class, f, 3, t0, t1, t2).objs);
    } else {
      return new RayObjects3(
          Ray.internal().call(null, () -> f.apply(null, null, null), 3, t0, t1, t2).objs);
    }
  }

  public static <T0, T1, T2, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_3_3<T0, T1, T2, R0, R1, R2> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(Ray.internal().call(null, RayFunc_3_3.class, f, 3, t0, t1, t2).objs);
    } else {
      return new RayObjects3(
          Ray.internal().call(null, () -> f.apply(null, null, null), 3, t0, t1, t2).objs);
    }
  }

  public static <T0, T1, T2, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_3_3<T0, T1, T2, R0, R1, R2> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(Ray.internal().call(null, RayFunc_3_3.class, f, 3, t0, t1, t2).objs);
    } else {
      return new RayObjects3(
          Ray.internal().call(null, () -> f.apply(null, null, null), 3, t0, t1, t2).objs);
    }
  }

  public static <T0, T1, T2, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_3_4<T0, T1, T2, R0, R1, R2, R3> f, T0 t0, T1 t1, T2 t2) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(Ray.internal().call(null, RayFunc_3_4.class, f, 4, t0, t1, t2).objs);
    } else {
      return new RayObjects4(
          Ray.internal().call(null, () -> f.apply(null, null, null), 4, t0, t1, t2).objs);
    }
  }

  public static <T0, T1, T2, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_3_4<T0, T1, T2, R0, R1, R2, R3> f, RayObject<T0> t0, T1 t1, T2 t2) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(Ray.internal().call(null, RayFunc_3_4.class, f, 4, t0, t1, t2).objs);
    } else {
      return new RayObjects4(
          Ray.internal().call(null, () -> f.apply(null, null, null), 4, t0, t1, t2).objs);
    }
  }

  public static <T0, T1, T2, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_3_4<T0, T1, T2, R0, R1, R2, R3> f, T0 t0, RayObject<T1> t1, T2 t2) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(Ray.internal().call(null, RayFunc_3_4.class, f, 4, t0, t1, t2).objs);
    } else {
      return new RayObjects4(
          Ray.internal().call(null, () -> f.apply(null, null, null), 4, t0, t1, t2).objs);
    }
  }

  public static <T0, T1, T2, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_3_4<T0, T1, T2, R0, R1, R2, R3> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(Ray.internal().call(null, RayFunc_3_4.class, f, 4, t0, t1, t2).objs);
    } else {
      return new RayObjects4(
          Ray.internal().call(null, () -> f.apply(null, null, null), 4, t0, t1, t2).objs);
    }
  }

  public static <T0, T1, T2, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_3_4<T0, T1, T2, R0, R1, R2, R3> f, T0 t0, T1 t1, RayObject<T2> t2) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(Ray.internal().call(null, RayFunc_3_4.class, f, 4, t0, t1, t2).objs);
    } else {
      return new RayObjects4(
          Ray.internal().call(null, () -> f.apply(null, null, null), 4, t0, t1, t2).objs);
    }
  }

  public static <T0, T1, T2, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_3_4<T0, T1, T2, R0, R1, R2, R3> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(Ray.internal().call(null, RayFunc_3_4.class, f, 4, t0, t1, t2).objs);
    } else {
      return new RayObjects4(
          Ray.internal().call(null, () -> f.apply(null, null, null), 4, t0, t1, t2).objs);
    }
  }

  public static <T0, T1, T2, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_3_4<T0, T1, T2, R0, R1, R2, R3> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(Ray.internal().call(null, RayFunc_3_4.class, f, 4, t0, t1, t2).objs);
    } else {
      return new RayObjects4(
          Ray.internal().call(null, () -> f.apply(null, null, null), 4, t0, t1, t2).objs);
    }
  }

  public static <T0, T1, T2, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_3_4<T0, T1, T2, R0, R1, R2, R3> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(Ray.internal().call(null, RayFunc_3_4.class, f, 4, t0, t1, t2).objs);
    } else {
      return new RayObjects4(
          Ray.internal().call(null, () -> f.apply(null, null, null), 4, t0, t1, t2).objs);
    }
  }

  public static <T0, T1, T2, T3, R> RayList<R> call_n(RayFunc_4_n_list<T0, T1, T2, T3, R> f,
      Integer returnCount, T0 t0, T1 t1, T2 t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_4_n_list.class, f, returnCount, t0, t1, t2, t3);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null), returnCount, t0,
              t1, t2, t3);
    }
  }

  public static <T0, T1, T2, T3, R> RayList<R> call_n(RayFunc_4_n_list<T0, T1, T2, T3, R> f,
      Integer returnCount, RayObject<T0> t0, T1 t1, T2 t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_4_n_list.class, f, returnCount, t0, t1, t2, t3);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null), returnCount, t0,
              t1, t2, t3);
    }
  }

  public static <T0, T1, T2, T3, R> RayList<R> call_n(RayFunc_4_n_list<T0, T1, T2, T3, R> f,
      Integer returnCount, T0 t0, RayObject<T1> t1, T2 t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_4_n_list.class, f, returnCount, t0, t1, t2, t3);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null), returnCount, t0,
              t1, t2, t3);
    }
  }

  public static <T0, T1, T2, T3, R> RayList<R> call_n(RayFunc_4_n_list<T0, T1, T2, T3, R> f,
      Integer returnCount, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_4_n_list.class, f, returnCount, t0, t1, t2, t3);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null), returnCount, t0,
              t1, t2, t3);
    }
  }

  public static <T0, T1, T2, T3, R> RayList<R> call_n(RayFunc_4_n_list<T0, T1, T2, T3, R> f,
      Integer returnCount, T0 t0, T1 t1, RayObject<T2> t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_4_n_list.class, f, returnCount, t0, t1, t2, t3);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null), returnCount, t0,
              t1, t2, t3);
    }
  }

  public static <T0, T1, T2, T3, R> RayList<R> call_n(RayFunc_4_n_list<T0, T1, T2, T3, R> f,
      Integer returnCount, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_4_n_list.class, f, returnCount, t0, t1, t2, t3);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null), returnCount, t0,
              t1, t2, t3);
    }
  }

  public static <T0, T1, T2, T3, R> RayList<R> call_n(RayFunc_4_n_list<T0, T1, T2, T3, R> f,
      Integer returnCount, T0 t0, T1 t1, T2 t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_4_n_list.class, f, returnCount, t0, t1, t2, t3);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null), returnCount, t0,
              t1, t2, t3);
    }
  }

  public static <T0, T1, T2, T3, R> RayList<R> call_n(RayFunc_4_n_list<T0, T1, T2, T3, R> f,
      Integer returnCount, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_4_n_list.class, f, returnCount, t0, t1, t2, t3);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null), returnCount, t0,
              t1, t2, t3);
    }
  }

  public static <T0, T1, T2, T3, R> RayList<R> call_n(RayFunc_4_n_list<T0, T1, T2, T3, R> f,
      Integer returnCount, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_4_n_list.class, f, returnCount, t0, t1, t2, t3);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null), returnCount, t0,
              t1, t2, t3);
    }
  }

  public static <T0, T1, T2, T3, R> RayList<R> call_n(RayFunc_4_n_list<T0, T1, T2, T3, R> f,
      Integer returnCount, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_4_n_list.class, f, returnCount, t0, t1, t2, t3);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null), returnCount, t0,
              t1, t2, t3);
    }
  }

  public static <T0, T1, T2, T3, R> RayList<R> call_n(RayFunc_4_n_list<T0, T1, T2, T3, R> f,
      Integer returnCount, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_4_n_list.class, f, returnCount, t0, t1, t2, t3);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null), returnCount, t0,
              t1, t2, t3);
    }
  }

  public static <T0, T1, T2, T3, R> RayList<R> call_n(RayFunc_4_n_list<T0, T1, T2, T3, R> f,
      Integer returnCount, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_4_n_list.class, f, returnCount, t0, t1, t2, t3);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null), returnCount, t0,
              t1, t2, t3);
    }
  }

  public static <T0, T1, T2, T3, R> RayList<R> call_n(RayFunc_4_n_list<T0, T1, T2, T3, R> f,
      Integer returnCount, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_4_n_list.class, f, returnCount, t0, t1, t2, t3);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null), returnCount, t0,
              t1, t2, t3);
    }
  }

  public static <T0, T1, T2, T3, R> RayList<R> call_n(RayFunc_4_n_list<T0, T1, T2, T3, R> f,
      Integer returnCount, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_4_n_list.class, f, returnCount, t0, t1, t2, t3);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null), returnCount, t0,
              t1, t2, t3);
    }
  }

  public static <T0, T1, T2, T3, R> RayList<R> call_n(RayFunc_4_n_list<T0, T1, T2, T3, R> f,
      Integer returnCount, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_4_n_list.class, f, returnCount, t0, t1, t2, t3);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null), returnCount, t0,
              t1, t2, t3);
    }
  }

  public static <T0, T1, T2, T3, R> RayList<R> call_n(RayFunc_4_n_list<T0, T1, T2, T3, R> f,
      Integer returnCount, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_4_n_list.class, f, returnCount, t0, t1, t2, t3);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null), returnCount, t0,
              t1, t2, t3);
    }
  }

  public static <T0, T1, T2, T3, R, RID> RayMap<RID, R> call_n(
      RayFunc_4_n<T0, T1, T2, T3, R, RID> f, Collection<RID> returnids, T0 t0, T1 t1, T2 t2,
      T3 t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_4_n.class, f, returnids, t0, t1, t2, t3);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null), returnids, t0,
              t1, t2, t3);
    }
  }

  public static <T0, T1, T2, T3, R, RID> RayMap<RID, R> call_n(
      RayFunc_4_n<T0, T1, T2, T3, R, RID> f, Collection<RID> returnids, RayObject<T0> t0, T1 t1,
      T2 t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_4_n.class, f, returnids, t0, t1, t2, t3);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null), returnids, t0,
              t1, t2, t3);
    }
  }

  public static <T0, T1, T2, T3, R, RID> RayMap<RID, R> call_n(
      RayFunc_4_n<T0, T1, T2, T3, R, RID> f, Collection<RID> returnids, T0 t0, RayObject<T1> t1,
      T2 t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_4_n.class, f, returnids, t0, t1, t2, t3);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null), returnids, t0,
              t1, t2, t3);
    }
  }

  public static <T0, T1, T2, T3, R, RID> RayMap<RID, R> call_n(
      RayFunc_4_n<T0, T1, T2, T3, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      RayObject<T1> t1, T2 t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_4_n.class, f, returnids, t0, t1, t2, t3);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null), returnids, t0,
              t1, t2, t3);
    }
  }

  public static <T0, T1, T2, T3, R, RID> RayMap<RID, R> call_n(
      RayFunc_4_n<T0, T1, T2, T3, R, RID> f, Collection<RID> returnids, T0 t0, T1 t1,
      RayObject<T2> t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_4_n.class, f, returnids, t0, t1, t2, t3);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null), returnids, t0,
              t1, t2, t3);
    }
  }

  public static <T0, T1, T2, T3, R, RID> RayMap<RID, R> call_n(
      RayFunc_4_n<T0, T1, T2, T3, R, RID> f, Collection<RID> returnids, RayObject<T0> t0, T1 t1,
      RayObject<T2> t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_4_n.class, f, returnids, t0, t1, t2, t3);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null), returnids, t0,
              t1, t2, t3);
    }
  }

  public static <T0, T1, T2, T3, R, RID> RayMap<RID, R> call_n(
      RayFunc_4_n<T0, T1, T2, T3, R, RID> f, Collection<RID> returnids, T0 t0, T1 t1, T2 t2,
      RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_4_n.class, f, returnids, t0, t1, t2, t3);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null), returnids, t0,
              t1, t2, t3);
    }
  }

  public static <T0, T1, T2, T3, R, RID> RayMap<RID, R> call_n(
      RayFunc_4_n<T0, T1, T2, T3, R, RID> f, Collection<RID> returnids, RayObject<T0> t0, T1 t1,
      T2 t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_4_n.class, f, returnids, t0, t1, t2, t3);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null), returnids, t0,
              t1, t2, t3);
    }
  }

  public static <T0, T1, T2, T3, R, RID> RayMap<RID, R> call_n(
      RayFunc_4_n<T0, T1, T2, T3, R, RID> f, Collection<RID> returnids, T0 t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_4_n.class, f, returnids, t0, t1, t2, t3);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null), returnids, t0,
              t1, t2, t3);
    }
  }

  public static <T0, T1, T2, T3, R, RID> RayMap<RID, R> call_n(
      RayFunc_4_n<T0, T1, T2, T3, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      RayObject<T1> t1, RayObject<T2> t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_4_n.class, f, returnids, t0, t1, t2, t3);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null), returnids, t0,
              t1, t2, t3);
    }
  }

  public static <T0, T1, T2, T3, R, RID> RayMap<RID, R> call_n(
      RayFunc_4_n<T0, T1, T2, T3, R, RID> f, Collection<RID> returnids, T0 t0, RayObject<T1> t1,
      T2 t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_4_n.class, f, returnids, t0, t1, t2, t3);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null), returnids, t0,
              t1, t2, t3);
    }
  }

  public static <T0, T1, T2, T3, R, RID> RayMap<RID, R> call_n(
      RayFunc_4_n<T0, T1, T2, T3, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      RayObject<T1> t1, T2 t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_4_n.class, f, returnids, t0, t1, t2, t3);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null), returnids, t0,
              t1, t2, t3);
    }
  }

  public static <T0, T1, T2, T3, R, RID> RayMap<RID, R> call_n(
      RayFunc_4_n<T0, T1, T2, T3, R, RID> f, Collection<RID> returnids, T0 t0, T1 t1,
      RayObject<T2> t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_4_n.class, f, returnids, t0, t1, t2, t3);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null), returnids, t0,
              t1, t2, t3);
    }
  }

  public static <T0, T1, T2, T3, R, RID> RayMap<RID, R> call_n(
      RayFunc_4_n<T0, T1, T2, T3, R, RID> f, Collection<RID> returnids, RayObject<T0> t0, T1 t1,
      RayObject<T2> t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_4_n.class, f, returnids, t0, t1, t2, t3);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null), returnids, t0,
              t1, t2, t3);
    }
  }

  public static <T0, T1, T2, T3, R, RID> RayMap<RID, R> call_n(
      RayFunc_4_n<T0, T1, T2, T3, R, RID> f, Collection<RID> returnids, T0 t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_4_n.class, f, returnids, t0, t1, t2, t3);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null), returnids, t0,
              t1, t2, t3);
    }
  }

  public static <T0, T1, T2, T3, R, RID> RayMap<RID, R> call_n(
      RayFunc_4_n<T0, T1, T2, T3, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_4_n.class, f, returnids, t0, t1, t2, t3);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null), returnids, t0,
              t1, t2, t3);
    }
  }

  public static <T0, T1, T2, T3, R0> RayObject<R0> call(RayFunc_4_1<T0, T1, T2, T3, R0> f, T0 t0,
      T1 t1, T2 t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_4_1.class, f, 1, t0, t1, t2, t3).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null), 1, t0, t1, t2, t3).objs[0];
    }
  }

  public static <T0, T1, T2, T3, R0> RayObject<R0> call(RayFunc_4_1<T0, T1, T2, T3, R0> f,
      RayObject<T0> t0, T1 t1, T2 t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_4_1.class, f, 1, t0, t1, t2, t3).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null), 1, t0, t1, t2, t3).objs[0];
    }
  }

  public static <T0, T1, T2, T3, R0> RayObject<R0> call(RayFunc_4_1<T0, T1, T2, T3, R0> f, T0 t0,
      RayObject<T1> t1, T2 t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_4_1.class, f, 1, t0, t1, t2, t3).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null), 1, t0, t1, t2, t3).objs[0];
    }
  }

  public static <T0, T1, T2, T3, R0> RayObject<R0> call(RayFunc_4_1<T0, T1, T2, T3, R0> f,
      RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_4_1.class, f, 1, t0, t1, t2, t3).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null), 1, t0, t1, t2, t3).objs[0];
    }
  }

  public static <T0, T1, T2, T3, R0> RayObject<R0> call(RayFunc_4_1<T0, T1, T2, T3, R0> f, T0 t0,
      T1 t1, RayObject<T2> t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_4_1.class, f, 1, t0, t1, t2, t3).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null), 1, t0, t1, t2, t3).objs[0];
    }
  }

  public static <T0, T1, T2, T3, R0> RayObject<R0> call(RayFunc_4_1<T0, T1, T2, T3, R0> f,
      RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_4_1.class, f, 1, t0, t1, t2, t3).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null), 1, t0, t1, t2, t3).objs[0];
    }
  }

  public static <T0, T1, T2, T3, R0> RayObject<R0> call(RayFunc_4_1<T0, T1, T2, T3, R0> f, T0 t0,
      T1 t1, T2 t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_4_1.class, f, 1, t0, t1, t2, t3).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null), 1, t0, t1, t2, t3).objs[0];
    }
  }

  public static <T0, T1, T2, T3, R0> RayObject<R0> call(RayFunc_4_1<T0, T1, T2, T3, R0> f,
      RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_4_1.class, f, 1, t0, t1, t2, t3).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null), 1, t0, t1, t2, t3).objs[0];
    }
  }

  public static <T0, T1, T2, T3, R0> RayObject<R0> call(RayFunc_4_1<T0, T1, T2, T3, R0> f, T0 t0,
      RayObject<T1> t1, RayObject<T2> t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_4_1.class, f, 1, t0, t1, t2, t3).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null), 1, t0, t1, t2, t3).objs[0];
    }
  }

  public static <T0, T1, T2, T3, R0> RayObject<R0> call(RayFunc_4_1<T0, T1, T2, T3, R0> f,
      RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_4_1.class, f, 1, t0, t1, t2, t3).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null), 1, t0, t1, t2, t3).objs[0];
    }
  }

  public static <T0, T1, T2, T3, R0> RayObject<R0> call(RayFunc_4_1<T0, T1, T2, T3, R0> f, T0 t0,
      RayObject<T1> t1, T2 t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_4_1.class, f, 1, t0, t1, t2, t3).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null), 1, t0, t1, t2, t3).objs[0];
    }
  }

  public static <T0, T1, T2, T3, R0> RayObject<R0> call(RayFunc_4_1<T0, T1, T2, T3, R0> f,
      RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_4_1.class, f, 1, t0, t1, t2, t3).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null), 1, t0, t1, t2, t3).objs[0];
    }
  }

  public static <T0, T1, T2, T3, R0> RayObject<R0> call(RayFunc_4_1<T0, T1, T2, T3, R0> f, T0 t0,
      T1 t1, RayObject<T2> t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_4_1.class, f, 1, t0, t1, t2, t3).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null), 1, t0, t1, t2, t3).objs[0];
    }
  }

  public static <T0, T1, T2, T3, R0> RayObject<R0> call(RayFunc_4_1<T0, T1, T2, T3, R0> f,
      RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_4_1.class, f, 1, t0, t1, t2, t3).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null), 1, t0, t1, t2, t3).objs[0];
    }
  }

  public static <T0, T1, T2, T3, R0> RayObject<R0> call(RayFunc_4_1<T0, T1, T2, T3, R0> f, T0 t0,
      RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_4_1.class, f, 1, t0, t1, t2, t3).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null), 1, t0, t1, t2, t3).objs[0];
    }
  }

  public static <T0, T1, T2, T3, R0> RayObject<R0> call(RayFunc_4_1<T0, T1, T2, T3, R0> f,
      RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_4_1.class, f, 1, t0, t1, t2, t3).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null), 1, t0, t1, t2, t3).objs[0];
    }
  }

  public static <T0, T1, T2, T3, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_4_2<T0, T1, T2, T3, R0, R1> f, T0 t0, T1 t1, T2 t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_4_2.class, f, 2, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects2(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 2, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_4_2<T0, T1, T2, T3, R0, R1> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_4_2.class, f, 2, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects2(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 2, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_4_2<T0, T1, T2, T3, R0, R1> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_4_2.class, f, 2, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects2(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 2, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_4_2<T0, T1, T2, T3, R0, R1> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_4_2.class, f, 2, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects2(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 2, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_4_2<T0, T1, T2, T3, R0, R1> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_4_2.class, f, 2, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects2(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 2, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_4_2<T0, T1, T2, T3, R0, R1> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_4_2.class, f, 2, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects2(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 2, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_4_2<T0, T1, T2, T3, R0, R1> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_4_2.class, f, 2, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects2(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 2, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_4_2<T0, T1, T2, T3, R0, R1> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_4_2.class, f, 2, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects2(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 2, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_4_2<T0, T1, T2, T3, R0, R1> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_4_2.class, f, 2, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects2(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 2, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_4_2<T0, T1, T2, T3, R0, R1> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2,
      T3 t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_4_2.class, f, 2, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects2(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 2, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_4_2<T0, T1, T2, T3, R0, R1> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_4_2.class, f, 2, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects2(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 2, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_4_2<T0, T1, T2, T3, R0, R1> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_4_2.class, f, 2, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects2(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 2, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_4_2<T0, T1, T2, T3, R0, R1> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_4_2.class, f, 2, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects2(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 2, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_4_2<T0, T1, T2, T3, R0, R1> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_4_2.class, f, 2, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects2(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 2, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_4_2<T0, T1, T2, T3, R0, R1> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_4_2.class, f, 2, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects2(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 2, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_4_2<T0, T1, T2, T3, R0, R1> f, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2,
      RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_4_2.class, f, 2, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects2(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 2, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_4_3<T0, T1, T2, T3, R0, R1, R2> f, T0 t0, T1 t1, T2 t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_4_3.class, f, 3, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects3(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 3, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_4_3<T0, T1, T2, T3, R0, R1, R2> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_4_3.class, f, 3, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects3(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 3, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_4_3<T0, T1, T2, T3, R0, R1, R2> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_4_3.class, f, 3, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects3(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 3, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_4_3<T0, T1, T2, T3, R0, R1, R2> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_4_3.class, f, 3, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects3(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 3, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_4_3<T0, T1, T2, T3, R0, R1, R2> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_4_3.class, f, 3, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects3(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 3, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_4_3<T0, T1, T2, T3, R0, R1, R2> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_4_3.class, f, 3, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects3(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 3, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_4_3<T0, T1, T2, T3, R0, R1, R2> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_4_3.class, f, 3, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects3(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 3, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_4_3<T0, T1, T2, T3, R0, R1, R2> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_4_3.class, f, 3, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects3(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 3, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_4_3<T0, T1, T2, T3, R0, R1, R2> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_4_3.class, f, 3, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects3(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 3, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_4_3<T0, T1, T2, T3, R0, R1, R2> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_4_3.class, f, 3, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects3(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 3, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_4_3<T0, T1, T2, T3, R0, R1, R2> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_4_3.class, f, 3, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects3(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 3, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_4_3<T0, T1, T2, T3, R0, R1, R2> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_4_3.class, f, 3, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects3(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 3, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_4_3<T0, T1, T2, T3, R0, R1, R2> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_4_3.class, f, 3, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects3(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 3, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_4_3<T0, T1, T2, T3, R0, R1, R2> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_4_3.class, f, 3, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects3(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 3, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_4_3<T0, T1, T2, T3, R0, R1, R2> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_4_3.class, f, 3, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects3(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 3, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_4_3<T0, T1, T2, T3, R0, R1, R2> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_4_3.class, f, 3, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects3(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 3, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_4_4<T0, T1, T2, T3, R0, R1, R2, R3> f, T0 t0, T1 t1, T2 t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_4_4.class, f, 4, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects4(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 4, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_4_4<T0, T1, T2, T3, R0, R1, R2, R3> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_4_4.class, f, 4, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects4(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 4, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_4_4<T0, T1, T2, T3, R0, R1, R2, R3> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_4_4.class, f, 4, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects4(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 4, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_4_4<T0, T1, T2, T3, R0, R1, R2, R3> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      T3 t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_4_4.class, f, 4, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects4(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 4, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_4_4<T0, T1, T2, T3, R0, R1, R2, R3> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_4_4.class, f, 4, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects4(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 4, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_4_4<T0, T1, T2, T3, R0, R1, R2, R3> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      T3 t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_4_4.class, f, 4, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects4(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 4, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_4_4<T0, T1, T2, T3, R0, R1, R2, R3> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_4_4.class, f, 4, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects4(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 4, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_4_4<T0, T1, T2, T3, R0, R1, R2, R3> f, RayObject<T0> t0, T1 t1, T2 t2,
      RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_4_4.class, f, 4, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects4(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 4, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_4_4<T0, T1, T2, T3, R0, R1, R2, R3> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      T3 t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_4_4.class, f, 4, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects4(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 4, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_4_4<T0, T1, T2, T3, R0, R1, R2, R3> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_4_4.class, f, 4, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects4(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 4, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_4_4<T0, T1, T2, T3, R0, R1, R2, R3> f, T0 t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_4_4.class, f, 4, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects4(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 4, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_4_4<T0, T1, T2, T3, R0, R1, R2, R3> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_4_4.class, f, 4, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects4(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 4, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_4_4<T0, T1, T2, T3, R0, R1, R2, R3> f, T0 t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_4_4.class, f, 4, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects4(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 4, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_4_4<T0, T1, T2, T3, R0, R1, R2, R3> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_4_4.class, f, 4, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects4(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 4, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_4_4<T0, T1, T2, T3, R0, R1, R2, R3> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_4_4.class, f, 4, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects4(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 4, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_4_4<T0, T1, T2, T3, R0, R1, R2, R3> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_4_4.class, f, 4, t0, t1, t2, t3).objs);
    } else {
      return new RayObjects4(
          Ray.internal().call(null, () -> f.apply(null, null, null, null), 4, t0, t1, t2, t3).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R> RayList<R> call_n(RayFunc_5_n_list<T0, T1, T2, T3, T4, R> f,
      Integer returnCount, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_5_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null), returnCount,
              t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R> RayList<R> call_n(RayFunc_5_n_list<T0, T1, T2, T3, T4, R> f,
      Integer returnCount, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_5_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null), returnCount,
              t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R> RayList<R> call_n(RayFunc_5_n_list<T0, T1, T2, T3, T4, R> f,
      Integer returnCount, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_5_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null), returnCount,
              t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R> RayList<R> call_n(RayFunc_5_n_list<T0, T1, T2, T3, T4, R> f,
      Integer returnCount, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_5_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null), returnCount,
              t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R> RayList<R> call_n(RayFunc_5_n_list<T0, T1, T2, T3, T4, R> f,
      Integer returnCount, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_5_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null), returnCount,
              t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R> RayList<R> call_n(RayFunc_5_n_list<T0, T1, T2, T3, T4, R> f,
      Integer returnCount, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_5_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null), returnCount,
              t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R> RayList<R> call_n(RayFunc_5_n_list<T0, T1, T2, T3, T4, R> f,
      Integer returnCount, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_5_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null), returnCount,
              t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R> RayList<R> call_n(RayFunc_5_n_list<T0, T1, T2, T3, T4, R> f,
      Integer returnCount, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_5_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null), returnCount,
              t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R> RayList<R> call_n(RayFunc_5_n_list<T0, T1, T2, T3, T4, R> f,
      Integer returnCount, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_5_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null), returnCount,
              t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R> RayList<R> call_n(RayFunc_5_n_list<T0, T1, T2, T3, T4, R> f,
      Integer returnCount, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_5_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null), returnCount,
              t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R> RayList<R> call_n(RayFunc_5_n_list<T0, T1, T2, T3, T4, R> f,
      Integer returnCount, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_5_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null), returnCount,
              t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R> RayList<R> call_n(RayFunc_5_n_list<T0, T1, T2, T3, T4, R> f,
      Integer returnCount, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_5_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null), returnCount,
              t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R> RayList<R> call_n(RayFunc_5_n_list<T0, T1, T2, T3, T4, R> f,
      Integer returnCount, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_5_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null), returnCount,
              t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R> RayList<R> call_n(RayFunc_5_n_list<T0, T1, T2, T3, T4, R> f,
      Integer returnCount, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_5_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null), returnCount,
              t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R> RayList<R> call_n(RayFunc_5_n_list<T0, T1, T2, T3, T4, R> f,
      Integer returnCount, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_5_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null), returnCount,
              t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R> RayList<R> call_n(RayFunc_5_n_list<T0, T1, T2, T3, T4, R> f,
      Integer returnCount, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_5_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null), returnCount,
              t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R> RayList<R> call_n(RayFunc_5_n_list<T0, T1, T2, T3, T4, R> f,
      Integer returnCount, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_5_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null), returnCount,
              t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R> RayList<R> call_n(RayFunc_5_n_list<T0, T1, T2, T3, T4, R> f,
      Integer returnCount, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_5_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null), returnCount,
              t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R> RayList<R> call_n(RayFunc_5_n_list<T0, T1, T2, T3, T4, R> f,
      Integer returnCount, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_5_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null), returnCount,
              t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R> RayList<R> call_n(RayFunc_5_n_list<T0, T1, T2, T3, T4, R> f,
      Integer returnCount, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_5_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null), returnCount,
              t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R> RayList<R> call_n(RayFunc_5_n_list<T0, T1, T2, T3, T4, R> f,
      Integer returnCount, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_5_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null), returnCount,
              t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R> RayList<R> call_n(RayFunc_5_n_list<T0, T1, T2, T3, T4, R> f,
      Integer returnCount, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3,
      T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_5_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null), returnCount,
              t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R> RayList<R> call_n(RayFunc_5_n_list<T0, T1, T2, T3, T4, R> f,
      Integer returnCount, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_5_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null), returnCount,
              t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R> RayList<R> call_n(RayFunc_5_n_list<T0, T1, T2, T3, T4, R> f,
      Integer returnCount, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_5_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null), returnCount,
              t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R> RayList<R> call_n(RayFunc_5_n_list<T0, T1, T2, T3, T4, R> f,
      Integer returnCount, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_5_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null), returnCount,
              t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R> RayList<R> call_n(RayFunc_5_n_list<T0, T1, T2, T3, T4, R> f,
      Integer returnCount, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3,
      RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_5_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null), returnCount,
              t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R> RayList<R> call_n(RayFunc_5_n_list<T0, T1, T2, T3, T4, R> f,
      Integer returnCount, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_5_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null), returnCount,
              t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R> RayList<R> call_n(RayFunc_5_n_list<T0, T1, T2, T3, T4, R> f,
      Integer returnCount, RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3,
      RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_5_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null), returnCount,
              t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R> RayList<R> call_n(RayFunc_5_n_list<T0, T1, T2, T3, T4, R> f,
      Integer returnCount, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_5_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null), returnCount,
              t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R> RayList<R> call_n(RayFunc_5_n_list<T0, T1, T2, T3, T4, R> f,
      Integer returnCount, RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3,
      RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_5_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null), returnCount,
              t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R> RayList<R> call_n(RayFunc_5_n_list<T0, T1, T2, T3, T4, R> f,
      Integer returnCount, T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3,
      RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_5_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null), returnCount,
              t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R> RayList<R> call_n(RayFunc_5_n_list<T0, T1, T2, T3, T4, R> f,
      Integer returnCount, RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3,
      RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_5_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null), returnCount,
              t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R, RID> RayMap<RID, R> call_n(
      RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f, Collection<RID> returnids, T0 t0, T1 t1, T2 t2,
      T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_5_n.class, f, returnids, t0, t1, t2, t3, t4);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R, RID> RayMap<RID, R> call_n(
      RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f, Collection<RID> returnids, RayObject<T0> t0, T1 t1,
      T2 t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_5_n.class, f, returnids, t0, t1, t2, t3, t4);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R, RID> RayMap<RID, R> call_n(
      RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f, Collection<RID> returnids, T0 t0, RayObject<T1> t1,
      T2 t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_5_n.class, f, returnids, t0, t1, t2, t3, t4);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R, RID> RayMap<RID, R> call_n(
      RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      RayObject<T1> t1, T2 t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_5_n.class, f, returnids, t0, t1, t2, t3, t4);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R, RID> RayMap<RID, R> call_n(
      RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f, Collection<RID> returnids, T0 t0, T1 t1,
      RayObject<T2> t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_5_n.class, f, returnids, t0, t1, t2, t3, t4);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R, RID> RayMap<RID, R> call_n(
      RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f, Collection<RID> returnids, RayObject<T0> t0, T1 t1,
      RayObject<T2> t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_5_n.class, f, returnids, t0, t1, t2, t3, t4);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R, RID> RayMap<RID, R> call_n(
      RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f, Collection<RID> returnids, T0 t0, T1 t1, T2 t2,
      RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_5_n.class, f, returnids, t0, t1, t2, t3, t4);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R, RID> RayMap<RID, R> call_n(
      RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f, Collection<RID> returnids, RayObject<T0> t0, T1 t1,
      T2 t2, RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_5_n.class, f, returnids, t0, t1, t2, t3, t4);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R, RID> RayMap<RID, R> call_n(
      RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f, Collection<RID> returnids, T0 t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_5_n.class, f, returnids, t0, t1, t2, t3, t4);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R, RID> RayMap<RID, R> call_n(
      RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_5_n.class, f, returnids, t0, t1, t2, t3, t4);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R, RID> RayMap<RID, R> call_n(
      RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f, Collection<RID> returnids, T0 t0, T1 t1, T2 t2,
      T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_5_n.class, f, returnids, t0, t1, t2, t3, t4);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R, RID> RayMap<RID, R> call_n(
      RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f, Collection<RID> returnids, RayObject<T0> t0, T1 t1,
      T2 t2, T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_5_n.class, f, returnids, t0, t1, t2, t3, t4);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R, RID> RayMap<RID, R> call_n(
      RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f, Collection<RID> returnids, T0 t0, RayObject<T1> t1,
      T2 t2, RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_5_n.class, f, returnids, t0, t1, t2, t3, t4);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R, RID> RayMap<RID, R> call_n(
      RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_5_n.class, f, returnids, t0, t1, t2, t3, t4);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R, RID> RayMap<RID, R> call_n(
      RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f, Collection<RID> returnids, T0 t0, RayObject<T1> t1,
      T2 t2, T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_5_n.class, f, returnids, t0, t1, t2, t3, t4);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R, RID> RayMap<RID, R> call_n(
      RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f, Collection<RID> returnids, T0 t0, T1 t1,
      RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_5_n.class, f, returnids, t0, t1, t2, t3, t4);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R, RID> RayMap<RID, R> call_n(
      RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_5_n.class, f, returnids, t0, t1, t2, t3, t4);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R, RID> RayMap<RID, R> call_n(
      RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f, Collection<RID> returnids, RayObject<T0> t0, T1 t1,
      RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_5_n.class, f, returnids, t0, t1, t2, t3, t4);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R, RID> RayMap<RID, R> call_n(
      RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f, Collection<RID> returnids, T0 t0, T1 t1,
      RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_5_n.class, f, returnids, t0, t1, t2, t3, t4);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R, RID> RayMap<RID, R> call_n(
      RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f, Collection<RID> returnids, RayObject<T0> t0, T1 t1,
      RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_5_n.class, f, returnids, t0, t1, t2, t3, t4);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R, RID> RayMap<RID, R> call_n(
      RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f, Collection<RID> returnids, T0 t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_5_n.class, f, returnids, t0, t1, t2, t3, t4);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R, RID> RayMap<RID, R> call_n(
      RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_5_n.class, f, returnids, t0, t1, t2, t3, t4);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R, RID> RayMap<RID, R> call_n(
      RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f, Collection<RID> returnids, T0 t0, T1 t1, T2 t2,
      RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_5_n.class, f, returnids, t0, t1, t2, t3, t4);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R, RID> RayMap<RID, R> call_n(
      RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f, Collection<RID> returnids, RayObject<T0> t0, T1 t1,
      T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_5_n.class, f, returnids, t0, t1, t2, t3, t4);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R, RID> RayMap<RID, R> call_n(
      RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f, Collection<RID> returnids, T0 t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_5_n.class, f, returnids, t0, t1, t2, t3, t4);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R, RID> RayMap<RID, R> call_n(
      RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_5_n.class, f, returnids, t0, t1, t2, t3, t4);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R, RID> RayMap<RID, R> call_n(
      RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f, Collection<RID> returnids, T0 t0, RayObject<T1> t1,
      T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_5_n.class, f, returnids, t0, t1, t2, t3, t4);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R, RID> RayMap<RID, R> call_n(
      RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_5_n.class, f, returnids, t0, t1, t2, t3, t4);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R, RID> RayMap<RID, R> call_n(
      RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f, Collection<RID> returnids, T0 t0, T1 t1,
      RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_5_n.class, f, returnids, t0, t1, t2, t3, t4);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R, RID> RayMap<RID, R> call_n(
      RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f, Collection<RID> returnids, RayObject<T0> t0, T1 t1,
      RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_5_n.class, f, returnids, t0, t1, t2, t3, t4);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R, RID> RayMap<RID, R> call_n(
      RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f, Collection<RID> returnids, T0 t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_5_n.class, f, returnids, t0, t1, t2, t3, t4);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R, RID> RayMap<RID, R> call_n(
      RayFunc_5_n<T0, T1, T2, T3, T4, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_5_n.class, f, returnids, t0, t1, t2, t3, t4);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4);
    }
  }

  public static <T0, T1, T2, T3, T4, R0> RayObject<R0> call(RayFunc_5_1<T0, T1, T2, T3, T4, R0> f,
      T0 t0, T1 t1, T2 t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_5_1.class, f, 1, t0, t1, t2, t3, t4).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 1, t0, t1, t2, t3, t4).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, R0> RayObject<R0> call(RayFunc_5_1<T0, T1, T2, T3, T4, R0> f,
      RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_5_1.class, f, 1, t0, t1, t2, t3, t4).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 1, t0, t1, t2, t3, t4).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, R0> RayObject<R0> call(RayFunc_5_1<T0, T1, T2, T3, T4, R0> f,
      T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_5_1.class, f, 1, t0, t1, t2, t3, t4).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 1, t0, t1, t2, t3, t4).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, R0> RayObject<R0> call(RayFunc_5_1<T0, T1, T2, T3, T4, R0> f,
      RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_5_1.class, f, 1, t0, t1, t2, t3, t4).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 1, t0, t1, t2, t3, t4).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, R0> RayObject<R0> call(RayFunc_5_1<T0, T1, T2, T3, T4, R0> f,
      T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_5_1.class, f, 1, t0, t1, t2, t3, t4).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 1, t0, t1, t2, t3, t4).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, R0> RayObject<R0> call(RayFunc_5_1<T0, T1, T2, T3, T4, R0> f,
      RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_5_1.class, f, 1, t0, t1, t2, t3, t4).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 1, t0, t1, t2, t3, t4).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, R0> RayObject<R0> call(RayFunc_5_1<T0, T1, T2, T3, T4, R0> f,
      T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_5_1.class, f, 1, t0, t1, t2, t3, t4).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 1, t0, t1, t2, t3, t4).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, R0> RayObject<R0> call(RayFunc_5_1<T0, T1, T2, T3, T4, R0> f,
      RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_5_1.class, f, 1, t0, t1, t2, t3, t4).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 1, t0, t1, t2, t3, t4).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, R0> RayObject<R0> call(RayFunc_5_1<T0, T1, T2, T3, T4, R0> f,
      T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_5_1.class, f, 1, t0, t1, t2, t3, t4).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 1, t0, t1, t2, t3, t4).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, R0> RayObject<R0> call(RayFunc_5_1<T0, T1, T2, T3, T4, R0> f,
      RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_5_1.class, f, 1, t0, t1, t2, t3, t4).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 1, t0, t1, t2, t3, t4).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, R0> RayObject<R0> call(RayFunc_5_1<T0, T1, T2, T3, T4, R0> f,
      T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_5_1.class, f, 1, t0, t1, t2, t3, t4).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 1, t0, t1, t2, t3, t4).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, R0> RayObject<R0> call(RayFunc_5_1<T0, T1, T2, T3, T4, R0> f,
      RayObject<T0> t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_5_1.class, f, 1, t0, t1, t2, t3, t4).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 1, t0, t1, t2, t3, t4).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, R0> RayObject<R0> call(RayFunc_5_1<T0, T1, T2, T3, T4, R0> f,
      T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_5_1.class, f, 1, t0, t1, t2, t3, t4).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 1, t0, t1, t2, t3, t4).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, R0> RayObject<R0> call(RayFunc_5_1<T0, T1, T2, T3, T4, R0> f,
      RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_5_1.class, f, 1, t0, t1, t2, t3, t4).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 1, t0, t1, t2, t3, t4).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, R0> RayObject<R0> call(RayFunc_5_1<T0, T1, T2, T3, T4, R0> f,
      T0 t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_5_1.class, f, 1, t0, t1, t2, t3, t4).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 1, t0, t1, t2, t3, t4).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, R0> RayObject<R0> call(RayFunc_5_1<T0, T1, T2, T3, T4, R0> f,
      T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_5_1.class, f, 1, t0, t1, t2, t3, t4).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 1, t0, t1, t2, t3, t4).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, R0> RayObject<R0> call(RayFunc_5_1<T0, T1, T2, T3, T4, R0> f,
      RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_5_1.class, f, 1, t0, t1, t2, t3, t4).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 1, t0, t1, t2, t3, t4).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, R0> RayObject<R0> call(RayFunc_5_1<T0, T1, T2, T3, T4, R0> f,
      RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_5_1.class, f, 1, t0, t1, t2, t3, t4).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 1, t0, t1, t2, t3, t4).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, R0> RayObject<R0> call(RayFunc_5_1<T0, T1, T2, T3, T4, R0> f,
      T0 t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_5_1.class, f, 1, t0, t1, t2, t3, t4).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 1, t0, t1, t2, t3, t4).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, R0> RayObject<R0> call(RayFunc_5_1<T0, T1, T2, T3, T4, R0> f,
      RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_5_1.class, f, 1, t0, t1, t2, t3, t4).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 1, t0, t1, t2, t3, t4).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, R0> RayObject<R0> call(RayFunc_5_1<T0, T1, T2, T3, T4, R0> f,
      T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_5_1.class, f, 1, t0, t1, t2, t3, t4).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 1, t0, t1, t2, t3, t4).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, R0> RayObject<R0> call(RayFunc_5_1<T0, T1, T2, T3, T4, R0> f,
      RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_5_1.class, f, 1, t0, t1, t2, t3, t4).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 1, t0, t1, t2, t3, t4).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, R0> RayObject<R0> call(RayFunc_5_1<T0, T1, T2, T3, T4, R0> f,
      T0 t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_5_1.class, f, 1, t0, t1, t2, t3, t4).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 1, t0, t1, t2, t3, t4).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, R0> RayObject<R0> call(RayFunc_5_1<T0, T1, T2, T3, T4, R0> f,
      RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_5_1.class, f, 1, t0, t1, t2, t3, t4).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 1, t0, t1, t2, t3, t4).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, R0> RayObject<R0> call(RayFunc_5_1<T0, T1, T2, T3, T4, R0> f,
      T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_5_1.class, f, 1, t0, t1, t2, t3, t4).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 1, t0, t1, t2, t3, t4).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, R0> RayObject<R0> call(RayFunc_5_1<T0, T1, T2, T3, T4, R0> f,
      RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_5_1.class, f, 1, t0, t1, t2, t3, t4).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 1, t0, t1, t2, t3, t4).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, R0> RayObject<R0> call(RayFunc_5_1<T0, T1, T2, T3, T4, R0> f,
      T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_5_1.class, f, 1, t0, t1, t2, t3, t4).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 1, t0, t1, t2, t3, t4).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, R0> RayObject<R0> call(RayFunc_5_1<T0, T1, T2, T3, T4, R0> f,
      RayObject<T0> t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_5_1.class, f, 1, t0, t1, t2, t3, t4).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 1, t0, t1, t2, t3, t4).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, R0> RayObject<R0> call(RayFunc_5_1<T0, T1, T2, T3, T4, R0> f,
      T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_5_1.class, f, 1, t0, t1, t2, t3, t4).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 1, t0, t1, t2, t3, t4).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, R0> RayObject<R0> call(RayFunc_5_1<T0, T1, T2, T3, T4, R0> f,
      RayObject<T0> t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_5_1.class, f, 1, t0, t1, t2, t3, t4).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 1, t0, t1, t2, t3, t4).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, R0> RayObject<R0> call(RayFunc_5_1<T0, T1, T2, T3, T4, R0> f,
      T0 t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_5_1.class, f, 1, t0, t1, t2, t3, t4).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 1, t0, t1, t2, t3, t4).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, R0> RayObject<R0> call(RayFunc_5_1<T0, T1, T2, T3, T4, R0> f,
      RayObject<T0> t0, RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_5_1.class, f, 1, t0, t1, t2, t3, t4).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 1, t0, t1, t2, t3, t4).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_5_2<T0, T1, T2, T3, T4, R0, R1> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_5_2.class, f, 2, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 2, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_5_2<T0, T1, T2, T3, T4, R0, R1> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_5_2.class, f, 2, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 2, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_5_2<T0, T1, T2, T3, T4, R0, R1> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_5_2.class, f, 2, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 2, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_5_2<T0, T1, T2, T3, T4, R0, R1> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3,
      T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_5_2.class, f, 2, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 2, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_5_2<T0, T1, T2, T3, T4, R0, R1> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_5_2.class, f, 2, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 2, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_5_2<T0, T1, T2, T3, T4, R0, R1> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3,
      T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_5_2.class, f, 2, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 2, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_5_2<T0, T1, T2, T3, T4, R0, R1> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_5_2.class, f, 2, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 2, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_5_2<T0, T1, T2, T3, T4, R0, R1> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3,
      T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_5_2.class, f, 2, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 2, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_5_2<T0, T1, T2, T3, T4, R0, R1> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3,
      T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_5_2.class, f, 2, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 2, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_5_2<T0, T1, T2, T3, T4, R0, R1> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_5_2.class, f, 2, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 2, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_5_2<T0, T1, T2, T3, T4, R0, R1> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_5_2.class, f, 2, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 2, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_5_2<T0, T1, T2, T3, T4, R0, R1> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3,
      RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_5_2.class, f, 2, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 2, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_5_2<T0, T1, T2, T3, T4, R0, R1> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3,
      T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_5_2.class, f, 2, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 2, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_5_2<T0, T1, T2, T3, T4, R0, R1> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_5_2.class, f, 2, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 2, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_5_2<T0, T1, T2, T3, T4, R0, R1> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3,
      RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_5_2.class, f, 2, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 2, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_5_2<T0, T1, T2, T3, T4, R0, R1> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3,
      T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_5_2.class, f, 2, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 2, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_5_2<T0, T1, T2, T3, T4, R0, R1> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3,
      RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_5_2.class, f, 2, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 2, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_5_2<T0, T1, T2, T3, T4, R0, R1> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_5_2.class, f, 2, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 2, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_5_2<T0, T1, T2, T3, T4, R0, R1> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3,
      RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_5_2.class, f, 2, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 2, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_5_2<T0, T1, T2, T3, T4, R0, R1> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3,
      RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_5_2.class, f, 2, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 2, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_5_2<T0, T1, T2, T3, T4, R0, R1> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_5_2.class, f, 2, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 2, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_5_2<T0, T1, T2, T3, T4, R0, R1> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_5_2.class, f, 2, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 2, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_5_2<T0, T1, T2, T3, T4, R0, R1> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3,
      RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_5_2.class, f, 2, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 2, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_5_2<T0, T1, T2, T3, T4, R0, R1> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3,
      RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_5_2.class, f, 2, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 2, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_5_2<T0, T1, T2, T3, T4, R0, R1> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3,
      RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_5_2.class, f, 2, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 2, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_5_2<T0, T1, T2, T3, T4, R0, R1> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_5_2.class, f, 2, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 2, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_5_2<T0, T1, T2, T3, T4, R0, R1> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3,
      RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_5_2.class, f, 2, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 2, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_5_2<T0, T1, T2, T3, T4, R0, R1> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_5_2.class, f, 2, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 2, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_5_2<T0, T1, T2, T3, T4, R0, R1> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3,
      RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_5_2.class, f, 2, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 2, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_5_2<T0, T1, T2, T3, T4, R0, R1> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_5_2.class, f, 2, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 2, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_5_2<T0, T1, T2, T3, T4, R0, R1> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_5_2.class, f, 2, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 2, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_5_2<T0, T1, T2, T3, T4, R0, R1> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_5_2.class, f, 2, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 2, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_5_3<T0, T1, T2, T3, T4, R0, R1, R2> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_5_3.class, f, 3, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 3, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_5_3<T0, T1, T2, T3, T4, R0, R1, R2> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_5_3.class, f, 3, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 3, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_5_3<T0, T1, T2, T3, T4, R0, R1, R2> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_5_3.class, f, 3, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 3, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_5_3<T0, T1, T2, T3, T4, R0, R1, R2> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_5_3.class, f, 3, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 3, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_5_3<T0, T1, T2, T3, T4, R0, R1, R2> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_5_3.class, f, 3, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 3, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_5_3<T0, T1, T2, T3, T4, R0, R1, R2> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_5_3.class, f, 3, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 3, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_5_3<T0, T1, T2, T3, T4, R0, R1, R2> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_5_3.class, f, 3, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 3, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_5_3<T0, T1, T2, T3, T4, R0, R1, R2> f, RayObject<T0> t0, T1 t1, T2 t2,
      RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_5_3.class, f, 3, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 3, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_5_3<T0, T1, T2, T3, T4, R0, R1, R2> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_5_3.class, f, 3, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 3, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_5_3<T0, T1, T2, T3, T4, R0, R1, R2> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_5_3.class, f, 3, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 3, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_5_3<T0, T1, T2, T3, T4, R0, R1, R2> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_5_3.class, f, 3, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 3, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_5_3<T0, T1, T2, T3, T4, R0, R1, R2> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3,
      RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_5_3.class, f, 3, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 3, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_5_3<T0, T1, T2, T3, T4, R0, R1, R2> f, T0 t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_5_3.class, f, 3, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 3, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_5_3<T0, T1, T2, T3, T4, R0, R1, R2> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_5_3.class, f, 3, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 3, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_5_3<T0, T1, T2, T3, T4, R0, R1, R2> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3,
      RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_5_3.class, f, 3, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 3, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_5_3<T0, T1, T2, T3, T4, R0, R1, R2> f, T0 t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_5_3.class, f, 3, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 3, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_5_3<T0, T1, T2, T3, T4, R0, R1, R2> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_5_3.class, f, 3, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 3, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_5_3<T0, T1, T2, T3, T4, R0, R1, R2> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_5_3.class, f, 3, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 3, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_5_3<T0, T1, T2, T3, T4, R0, R1, R2> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3,
      RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_5_3.class, f, 3, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 3, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_5_3<T0, T1, T2, T3, T4, R0, R1, R2> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_5_3.class, f, 3, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 3, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_5_3<T0, T1, T2, T3, T4, R0, R1, R2> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_5_3.class, f, 3, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 3, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_5_3<T0, T1, T2, T3, T4, R0, R1, R2> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_5_3.class, f, 3, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 3, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_5_3<T0, T1, T2, T3, T4, R0, R1, R2> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3,
      RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_5_3.class, f, 3, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 3, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_5_3<T0, T1, T2, T3, T4, R0, R1, R2> f, RayObject<T0> t0, T1 t1, T2 t2,
      RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_5_3.class, f, 3, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 3, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_5_3<T0, T1, T2, T3, T4, R0, R1, R2> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_5_3.class, f, 3, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 3, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_5_3<T0, T1, T2, T3, T4, R0, R1, R2> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_5_3.class, f, 3, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 3, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_5_3<T0, T1, T2, T3, T4, R0, R1, R2> f, T0 t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_5_3.class, f, 3, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 3, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_5_3<T0, T1, T2, T3, T4, R0, R1, R2> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_5_3.class, f, 3, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 3, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_5_3<T0, T1, T2, T3, T4, R0, R1, R2> f, T0 t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_5_3.class, f, 3, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 3, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_5_3<T0, T1, T2, T3, T4, R0, R1, R2> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_5_3.class, f, 3, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 3, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_5_3<T0, T1, T2, T3, T4, R0, R1, R2> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_5_3.class, f, 3, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 3, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_5_3<T0, T1, T2, T3, T4, R0, R1, R2> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_5_3.class, f, 3, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 3, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_5_4<T0, T1, T2, T3, T4, R0, R1, R2, R3> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_5_4.class, f, 4, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 4, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_5_4<T0, T1, T2, T3, T4, R0, R1, R2, R3> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3,
      T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_5_4.class, f, 4, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 4, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_5_4<T0, T1, T2, T3, T4, R0, R1, R2, R3> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3,
      T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_5_4.class, f, 4, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 4, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_5_4<T0, T1, T2, T3, T4, R0, R1, R2, R3> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_5_4.class, f, 4, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 4, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_5_4<T0, T1, T2, T3, T4, R0, R1, R2, R3> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3,
      T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_5_4.class, f, 4, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 4, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_5_4<T0, T1, T2, T3, T4, R0, R1, R2, R3> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_5_4.class, f, 4, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 4, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_5_4<T0, T1, T2, T3, T4, R0, R1, R2, R3> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3,
      T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_5_4.class, f, 4, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 4, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_5_4<T0, T1, T2, T3, T4, R0, R1, R2, R3> f, RayObject<T0> t0, T1 t1, T2 t2,
      RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_5_4.class, f, 4, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 4, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_5_4<T0, T1, T2, T3, T4, R0, R1, R2, R3> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_5_4.class, f, 4, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 4, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_5_4<T0, T1, T2, T3, T4, R0, R1, R2, R3> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_5_4.class, f, 4, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 4, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_5_4<T0, T1, T2, T3, T4, R0, R1, R2, R3> f, T0 t0, T1 t1, T2 t2, T3 t3,
      RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_5_4.class, f, 4, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 4, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_5_4<T0, T1, T2, T3, T4, R0, R1, R2, R3> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3,
      RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_5_4.class, f, 4, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 4, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_5_4<T0, T1, T2, T3, T4, R0, R1, R2, R3> f, T0 t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_5_4.class, f, 4, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 4, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_5_4<T0, T1, T2, T3, T4, R0, R1, R2, R3> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_5_4.class, f, 4, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 4, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_5_4<T0, T1, T2, T3, T4, R0, R1, R2, R3> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3,
      RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_5_4.class, f, 4, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 4, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_5_4<T0, T1, T2, T3, T4, R0, R1, R2, R3> f, T0 t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_5_4.class, f, 4, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 4, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_5_4<T0, T1, T2, T3, T4, R0, R1, R2, R3> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_5_4.class, f, 4, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 4, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_5_4<T0, T1, T2, T3, T4, R0, R1, R2, R3> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_5_4.class, f, 4, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 4, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_5_4<T0, T1, T2, T3, T4, R0, R1, R2, R3> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3,
      RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_5_4.class, f, 4, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 4, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_5_4<T0, T1, T2, T3, T4, R0, R1, R2, R3> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_5_4.class, f, 4, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 4, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_5_4<T0, T1, T2, T3, T4, R0, R1, R2, R3> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_5_4.class, f, 4, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 4, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_5_4<T0, T1, T2, T3, T4, R0, R1, R2, R3> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3, T4 t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_5_4.class, f, 4, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 4, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_5_4<T0, T1, T2, T3, T4, R0, R1, R2, R3> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3,
      RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_5_4.class, f, 4, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 4, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_5_4<T0, T1, T2, T3, T4, R0, R1, R2, R3> f, RayObject<T0> t0, T1 t1, T2 t2,
      RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_5_4.class, f, 4, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 4, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_5_4<T0, T1, T2, T3, T4, R0, R1, R2, R3> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_5_4.class, f, 4, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 4, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_5_4<T0, T1, T2, T3, T4, R0, R1, R2, R3> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_5_4.class, f, 4, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 4, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_5_4<T0, T1, T2, T3, T4, R0, R1, R2, R3> f, T0 t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_5_4.class, f, 4, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 4, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_5_4<T0, T1, T2, T3, T4, R0, R1, R2, R3> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_5_4.class, f, 4, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 4, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_5_4<T0, T1, T2, T3, T4, R0, R1, R2, R3> f, T0 t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_5_4.class, f, 4, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 4, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_5_4<T0, T1, T2, T3, T4, R0, R1, R2, R3> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_5_4.class, f, 4, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 4, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_5_4<T0, T1, T2, T3, T4, R0, R1, R2, R3> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_5_4.class, f, 4, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 4, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_5_4<T0, T1, T2, T3, T4, R0, R1, R2, R3> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_5_4.class, f, 4, t0, t1, t2, t3, t4).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null), 4, t0, t1, t2, t3, t4).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, T0 t0, T1 t1, T2 t2,
      T3 t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, RayObject<T0> t0, T1 t1,
      T2 t2, T3 t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, T0 t0, RayObject<T1> t1,
      T2 t2, T3 t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, RayObject<T0> t0,
      RayObject<T1> t1, T2 t2, T3 t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, T0 t0, T1 t1,
      RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, RayObject<T0> t0, T1 t1,
      RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, T0 t0, T1 t1, T2 t2,
      RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, RayObject<T0> t0, T1 t1,
      T2 t2, RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, T0 t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, RayObject<T0> t0,
      RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, T0 t0, T1 t1, T2 t2,
      T3 t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, RayObject<T0> t0, T1 t1,
      T2 t2, T3 t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, T0 t0, RayObject<T1> t1,
      T2 t2, RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, RayObject<T0> t0,
      RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, T0 t0, T1 t1, T2 t2,
      T3 t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, RayObject<T0> t0, T1 t1,
      T2 t2, T3 t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, T0 t0, RayObject<T1> t1,
      T2 t2, T3 t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, T0 t0, T1 t1,
      RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, RayObject<T0> t0,
      RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, RayObject<T0> t0, T1 t1,
      RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, T0 t0, RayObject<T1> t1,
      T2 t2, T3 t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, T0 t0, T1 t1,
      RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, RayObject<T0> t0,
      RayObject<T1> t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, RayObject<T0> t0, T1 t1,
      RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, T0 t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, RayObject<T0> t0,
      RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, T0 t0, T1 t1,
      RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, T0 t0, T1 t1, T2 t2,
      RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, RayObject<T0> t0, T1 t1,
      RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, RayObject<T0> t0, T1 t1,
      T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, T0 t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, RayObject<T0> t0,
      RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, T0 t0, T1 t1, T2 t2,
      RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, RayObject<T0> t0, T1 t1,
      T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, T0 t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, T0 t0, RayObject<T1> t1,
      T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, RayObject<T0> t0,
      RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, RayObject<T0> t0,
      RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, T0 t0, T1 t1, T2 t2,
      T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, RayObject<T0> t0, T1 t1,
      T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, T0 t0, RayObject<T1> t1,
      T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, T0 t0, T1 t1,
      RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, RayObject<T0> t0,
      RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, RayObject<T0> t0, T1 t1,
      RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, T0 t0, RayObject<T1> t1,
      T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, T0 t0, T1 t1,
      RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, RayObject<T0> t0,
      RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, RayObject<T0> t0, T1 t1,
      RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, T0 t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, RayObject<T0> t0,
      RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, T0 t0, T1 t1,
      RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, RayObject<T0> t0, T1 t1,
      RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, T0 t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, RayObject<T0> t0,
      RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, T0 t0, T1 t1, T2 t2,
      RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, RayObject<T0> t0, T1 t1,
      T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, T0 t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, RayObject<T0> t0,
      RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, T0 t0, RayObject<T1> t1,
      T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, RayObject<T0> t0,
      RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, T0 t0, T1 t1,
      RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, RayObject<T0> t0, T1 t1,
      RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, T0 t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R> RayList<R> call_n(
      RayFunc_6_n_list<T0, T1, T2, T3, T4, T5, R> f, Integer returnCount, RayObject<T0> t0,
      RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnIndices(null, RayFunc_6_n_list.class, f, returnCount, t0, t1, t2, t3,
              t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnIndices(null, () -> f.apply(null, null, null, null, null, null),
              returnCount, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, T0 t0, T1 t1, T2 t2,
      T3 t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      T1 t1, T2 t2, T3 t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, T0 t0,
      RayObject<T1> t1, T2 t2, T3 t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      RayObject<T1> t1, T2 t2, T3 t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, T0 t0, T1 t1,
      RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      T1 t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, T0 t0, T1 t1, T2 t2,
      RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      T1 t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, T0 t0,
      RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, T0 t0, T1 t1, T2 t2,
      T3 t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      T1 t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, T0 t0,
      RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, T0 t0, T1 t1, T2 t2,
      T3 t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      T1 t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, T0 t0,
      RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, T0 t0, T1 t1,
      RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, T0 t0,
      RayObject<T1> t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, T0 t0, T1 t1,
      RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      RayObject<T1> t1, T2 t2, T3 t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, T0 t0,
      RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, T0 t0, T1 t1,
      RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, T0 t0, T1 t1, T2 t2,
      RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      T1 t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, T0 t0,
      RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, T0 t0, T1 t1, T2 t2,
      RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      T1 t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, T0 t0,
      RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, T0 t0,
      RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      RayObject<T1> t1, RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, T0 t0, T1 t1, T2 t2,
      T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      T1 t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, T0 t0,
      RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, T0 t0, T1 t1,
      RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      RayObject<T1> t1, T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, T0 t0,
      RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, T0 t0, T1 t1,
      RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      RayObject<T1> t1, T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      T1 t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, T0 t0,
      RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, T0 t0, T1 t1,
      RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      T1 t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, T0 t0,
      RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, T0 t0, T1 t1, T2 t2,
      RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      T1 t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, T0 t0,
      RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      RayObject<T1> t1, RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, T0 t0,
      RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      RayObject<T1> t1, T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, T0 t0, T1 t1,
      RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      T1 t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, T0 t0,
      RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R, RID> RayMap<RID, R> call_n(
      RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RID> f, Collection<RID> returnids, RayObject<T0> t0,
      RayObject<T1> t1, RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal()
          .callWithReturnLabels(null, RayFunc_6_n.class, f, returnids, t0, t1, t2, t3, t4, t5);
    } else {
      return Ray.internal()
          .callWithReturnLabels(null, () -> f.apply(null, null, null, null, null, null, null),
              returnids, t0, t1, t2, t3, t4, t5);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4,
      T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4,
      T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3,
      T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4,
      T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3,
      T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4,
      T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3,
      T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3,
      T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4,
      T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3,
      RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3,
      T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4,
      RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4,
      RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3,
      RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3,
      T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3,
      RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4,
      RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3,
      RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3,
      T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3,
      RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4,
      RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3,
      RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3,
      T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3,
      RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3,
      RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4,
      RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3,
      T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3,
      T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3,
      RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4,
      RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3,
      RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3,
      T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3,
      RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3,
      RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3,
      T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2, T3 t3,
      RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3,
      RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2, T3 t3,
      RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3,
      RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, RayObject<T0> t0, T1 t1, T2 t2, RayObject<T3> t3,
      RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2, T3 t3,
      RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, T0 t0, RayObject<T1> t1, T2 t2, RayObject<T3> t3,
      RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, T0 t0, T1 t1, RayObject<T2> t2, RayObject<T3> t3,
      RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0> RayObject<R0> call(
      RayFunc_6_1<T0, T1, T2, T3, T4, T5, R0> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return Ray.internal().call(null, RayFunc_6_1.class, f, 1, t0, t1, t2, t3, t4, t5).objs[0];
    } else {
      return Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 1, t0, t1, t2, t3, t4,
              t5).objs[0];
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4,
      T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4,
      T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      T3 t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4,
      T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      T3 t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4,
      T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, RayObject<T0> t0, T1 t1, T2 t2,
      RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      T3 t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4,
      T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3,
      RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, T0 t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4,
      RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3, T4 t4,
      RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3,
      RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, T0 t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      T3 t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3, T4 t4,
      RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3,
      RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      T3 t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      T3 t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3, T4 t4,
      RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3,
      RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      T3 t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, RayObject<T0> t0, T1 t1, T2 t2,
      RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      T3 t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3, T4 t4,
      RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, RayObject<T0> t0, T1 t1, T2 t2,
      RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      T3 t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, T0 t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, T0 t0, T1 t1, T2 t2, T3 t3, RayObject<T4> t4,
      RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3,
      RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, T0 t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, T0 t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3,
      RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, T0 t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3,
      RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3,
      RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, RayObject<T0> t0, T1 t1, T2 t2,
      RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, T0 t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, T0 t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1> RayObjects2<R0, R1> call_2(
      RayFunc_6_2<T0, T1, T2, T3, T4, T5, R0, R1> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects2(
          Ray.internal().call(null, RayFunc_6_2.class, f, 2, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects2(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 2, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3,
      T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3,
      T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      T3 t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3,
      T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      T3 t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3,
      T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, RayObject<T0> t0, T1 t1, T2 t2,
      RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      T3 t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, T0 t0, T1 t1, T2 t2, T3 t3,
      RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3,
      RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, T0 t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4,
      RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3,
      T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3,
      RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, T0 t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      T3 t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3,
      T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3,
      RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      T3 t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      T3 t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3,
      T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3,
      RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      T3 t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, RayObject<T0> t0, T1 t1, T2 t2,
      RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      T3 t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3,
      T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, RayObject<T0> t0, T1 t1, T2 t2,
      RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      T3 t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, T0 t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, T0 t0, T1 t1, T2 t2, T3 t3,
      RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3,
      RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, T0 t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, T0 t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3,
      RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, T0 t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3,
      RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3,
      RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, RayObject<T0> t0, T1 t1, T2 t2,
      RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, T0 t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, RayObject<T0> t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, T0 t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, RayObject<T0> t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, T0 t0, RayObject<T1> t1, RayObject<T2> t2,
      RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2> RayObjects3<R0, R1, R2> call_3(
      RayFunc_6_3<T0, T1, T2, T3, T4, T5, R0, R1, R2> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects3(
          Ray.internal().call(null, RayFunc_6_3.class, f, 3, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects3(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 3, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4,
      T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3,
      T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3,
      T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, RayObject<T0> t0, RayObject<T1> t1,
      T2 t2, T3 t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3,
      T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, RayObject<T0> t0, T1 t1,
      RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3,
      T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, RayObject<T0> t0, T1 t1, T2 t2,
      RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, T0 t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, T0 t0, T1 t1, T2 t2, T3 t3,
      RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3,
      RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, T0 t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, RayObject<T0> t0, RayObject<T1> t1,
      T2 t2, RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4,
      RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3,
      T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3,
      RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, T0 t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, RayObject<T0> t0, RayObject<T1> t1,
      T2 t2, T3 t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, RayObject<T0> t0, T1 t1,
      RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3,
      T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3,
      RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, RayObject<T0> t0, RayObject<T1> t1,
      T2 t2, T3 t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, RayObject<T0> t0, T1 t1,
      RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, T0 t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3, T4 t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3,
      T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3,
      RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, RayObject<T0> t0, T1 t1,
      RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, RayObject<T0> t0, T1 t1, T2 t2,
      RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, T0 t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3,
      T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, RayObject<T0> t0, T1 t1, T2 t2,
      RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, T0 t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, T0 t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, RayObject<T0> t0, RayObject<T1> t1,
      T2 t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, T0 t0, T1 t1, T2 t2, T3 t3,
      RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, RayObject<T0> t0, T1 t1, T2 t2, T3 t3,
      RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, T0 t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, T0 t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, RayObject<T0> t0, RayObject<T1> t1,
      T2 t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, RayObject<T0> t0, T1 t1,
      RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, T0 t0, RayObject<T1> t1, T2 t2, T3 t3,
      RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, T0 t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, RayObject<T0> t0, RayObject<T1> t1,
      T2 t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, RayObject<T0> t0, T1 t1,
      RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, T0 t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, T5 t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, T0 t0, T1 t1, RayObject<T2> t2, T3 t3,
      RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, RayObject<T0> t0, T1 t1,
      RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, T0 t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3, T4 t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, T0 t0, T1 t1, T2 t2, RayObject<T3> t3,
      RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, RayObject<T0> t0, T1 t1, T2 t2,
      RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, T0 t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, T3 t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, T0 t0, RayObject<T1> t1, T2 t2,
      RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, RayObject<T0> t0, RayObject<T1> t1,
      T2 t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, T0 t0, T1 t1, RayObject<T2> t2,
      RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, RayObject<T0> t0, T1 t1,
      RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, T0 t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }

  public static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> RayObjects4<R0, R1, R2, R3> call_4(
      RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f, RayObject<T0> t0, RayObject<T1> t1,
      RayObject<T2> t2, RayObject<T3> t3, RayObject<T4> t4, RayObject<T5> t5) {
    if (Ray.isRemoteLambda()) {
      return new RayObjects4(
          Ray.internal().call(null, RayFunc_6_4.class, f, 4, t0, t1, t2, t3, t4, t5).objs);
    } else {
      return new RayObjects4(Ray.internal()
          .call(null, () -> f.apply(null, null, null, null, null, null), 4, t0, t1, t2, t3, t4,
              t5).objs);
    }
  }
}
