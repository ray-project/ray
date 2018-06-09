package org.ray.api.funcs;

import org.apache.commons.lang3.SerializationUtils;
import org.ray.api.internal.RayFunc;
import org.ray.api.returns.MultipleReturns3;

@FunctionalInterface
public interface RayFunc_1_3<T0, R0, R1, R2> extends RayFunc {

  static <T0, R0, R1, R2> MultipleReturns3<R0, R1, R2> execute(Object[] args) throws Throwable {
    String name = (String) args[args.length - 2];
    assert (name.equals(RayFunc_1_3.class.getName()));
    byte[] funcBytes = (byte[]) args[args.length - 1];
    RayFunc_1_3<T0, R0, R1, R2> f = SerializationUtils.deserialize(funcBytes);
    return f.apply((T0) args[0]);
  }

  MultipleReturns3<R0, R1, R2> apply(T0 t0) throws Throwable;

}
