package org.ray.api.funcs;

import org.apache.commons.lang3.SerializationUtils;
import org.ray.api.internal.RayFunc;
import org.ray.api.returns.MultipleReturns2;

@FunctionalInterface
public interface RayFunc_0_2<R0, R1> extends RayFunc {

  MultipleReturns2<R0, R1> apply() throws Throwable;

  static <R0, R1> MultipleReturns2<R0, R1> execute(Object[] args) throws Throwable {
    String name = (String) args[args.length - 2];
    assert (name.equals(RayFunc_0_2.class.getName()));
    byte[] funcBytes = (byte[]) args[args.length - 1];
    RayFunc_0_2<R0, R1> f = SerializationUtils.deserialize(funcBytes);
    return f.apply();
  }

}
