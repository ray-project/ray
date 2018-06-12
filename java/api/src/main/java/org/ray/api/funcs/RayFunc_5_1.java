package org.ray.api.funcs;

import org.apache.commons.lang3.SerializationUtils;
import org.ray.api.internal.RayFunc;

@FunctionalInterface
public interface RayFunc_5_1<T0, T1, T2, T3, T4, R0> extends RayFunc {

  static <T0, T1, T2, T3, T4, R0> R0 execute(Object[] args) throws Throwable {
    String name = (String) args[args.length - 2];
    assert (name.equals(RayFunc_5_1.class.getName()));
    byte[] funcBytes = (byte[]) args[args.length - 1];
    RayFunc_5_1<T0, T1, T2, T3, T4, R0> f = SerializationUtils.deserialize(funcBytes);
    return f.apply((T0) args[0], (T1) args[1], (T2) args[2], (T3) args[3], (T4) args[4]);
  }

  R0 apply(T0 t0, T1 t1, T2 t2, T3 t3, T4 t4) throws Throwable;

}
