package org.ray.api.funcs;

import org.apache.commons.lang3.SerializationUtils;
import org.ray.api.internal.RayFunc;
import org.ray.api.returns.MultipleReturns4;

@FunctionalInterface
public interface RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> extends RayFunc {

  MultipleReturns4<R0, R1, R2, R3> apply(T0 t0, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5) throws Throwable;

  static <T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> MultipleReturns4<R0, R1, R2, R3> execute(
      Object[] args) throws Throwable {
    String name = (String) args[args.length - 2];
    assert (name.equals(RayFunc_6_4.class.getName()));
    byte[] funcBytes = (byte[]) args[args.length - 1];
    RayFunc_6_4<T0, T1, T2, T3, T4, T5, R0, R1, R2, R3> f = SerializationUtils
        .deserialize(funcBytes);
    return f
        .apply((T0) args[0], (T1) args[1], (T2) args[2], (T3) args[3], (T4) args[4], (T5) args[5]);
  }

}
