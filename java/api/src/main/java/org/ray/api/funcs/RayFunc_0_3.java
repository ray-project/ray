package org.ray.api.funcs;

import org.apache.commons.lang3.SerializationUtils;
import org.ray.api.internal.RayFunc;
import org.ray.api.returns.MultipleReturns3;

@FunctionalInterface
public interface RayFunc_0_3<R0, R1, R2> extends RayFunc {

  MultipleReturns3<R0, R1, R2> apply() throws Throwable;

  static <R0, R1, R2> MultipleReturns3<R0, R1, R2> execute(Object[] args) throws Throwable {
    String name = (String) args[args.length - 2];
    assert (name.equals(RayFunc_0_3.class.getName()));
    byte[] funcBytes = (byte[]) args[args.length - 1];
    RayFunc_0_3<R0, R1, R2> f = SerializationUtils.deserialize(funcBytes);
    return f.apply();
  }

}
