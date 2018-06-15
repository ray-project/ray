package org.ray.api.funcs;

import org.apache.commons.lang3.SerializationUtils;
import org.ray.api.internal.RayFunc;
import org.ray.api.returns.MultipleReturns4;

@FunctionalInterface
public interface RayFunc_2_4<T0, T1, R0, R1, R2, R3> extends RayFunc {

  MultipleReturns4<R0, R1, R2, R3> apply(T0 t0, T1 t1) throws Throwable;

}
