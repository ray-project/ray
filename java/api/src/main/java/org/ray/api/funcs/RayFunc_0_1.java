package org.ray.api.funcs;

import org.apache.commons.lang3.SerializationUtils;
import org.ray.api.internal.RayFunc;

@FunctionalInterface
public interface RayFunc_0_1<R0> extends RayFunc {

  R0 apply() throws Throwable;

  static <R0> R0 execute(Object[] args) throws Throwable {
    String name = (String) args[args.length - 2];
    assert (name.equals(RayFunc_0_1.class.getName()));
    byte[] funcBytes = (byte[]) args[args.length - 1];
    RayFunc_0_1<R0> f = SerializationUtils.deserialize(funcBytes);
    return f.apply();
  }

}
