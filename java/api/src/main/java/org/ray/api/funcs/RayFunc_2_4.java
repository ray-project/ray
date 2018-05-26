package org.ray.api.funcs;

import org.apache.commons.lang3.SerializationUtils;
import org.ray.api.internal.RayFunc;
import org.ray.api.returns.MultipleReturns4;

@FunctionalInterface
public interface RayFunc_2_4<T0, T1, R0, R1, R2, R3> extends RayFunc {

  MultipleReturns4<R0, R1, R2, R3> apply(T0 t0, T1 t1) throws Throwable;

  static <T0, T1, R0, R1, R2, R3> MultipleReturns4<R0, R1, R2, R3> execute(Object[] args)
      throws Throwable {
    String name = (String) args[args.length - 2];
    assert (name.equals(RayFunc_2_4.class.getName()));
    byte[] funcBytes = (byte[]) args[args.length - 1];
    RayFunc_2_4<T0, T1, R0, R1, R2, R3> f = SerializationUtils.deserialize(funcBytes);
    return f.apply((T0) args[0], (T1) args[1]);
  }

}
