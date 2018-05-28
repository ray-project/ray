package org.ray.api.funcs;

import org.apache.commons.lang3.SerializationUtils;
import org.ray.api.internal.RayFunc;

@FunctionalInterface
public interface RayFunc_1_1<T0, R0> extends RayFunc {

  R0 apply(T0 t0) throws Throwable;

  static <T0, R0> R0 execute(Object[] args) throws Throwable {
    String name = (String) args[args.length - 2];
    assert (name.equals(RayFunc_1_1.class.getName()));
    byte[] funcBytes = (byte[]) args[args.length - 1];
    RayFunc_1_1<T0, R0> f = SerializationUtils.deserialize(funcBytes);
    return f.apply((T0) args[0]);
  }

}
