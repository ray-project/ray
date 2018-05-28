package org.ray.api.funcs;

import org.apache.commons.lang3.SerializationUtils;
import org.ray.api.internal.RayFunc;
import org.ray.api.returns.MultipleReturns3;

@FunctionalInterface
public interface RayFunc_3_3<T0, T1, T2, R0, R1, R2> extends RayFunc {

  MultipleReturns3<R0, R1, R2> apply(T0 t0, T1 t1, T2 t2) throws Throwable;

  static <T0, T1, T2, R0, R1, R2> MultipleReturns3<R0, R1, R2> execute(Object[] args)
      throws Throwable {
    String name = (String) args[args.length - 2];
    assert (name.equals(RayFunc_3_3.class.getName()));
    byte[] funcBytes = (byte[]) args[args.length - 1];
    RayFunc_3_3<T0, T1, T2, R0, R1, R2> f = SerializationUtils.deserialize(funcBytes);
    return f.apply((T0) args[0], (T1) args[1], (T2) args[2]);
  }

}
