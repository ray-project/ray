package org.ray.api.funcs;

import org.apache.commons.lang3.SerializationUtils;
import org.ray.api.internal.RayFunc;
import org.ray.api.returns.MultipleReturns4;

@FunctionalInterface
public interface RayFunc_1_4<T0, R0, R1, R2, R3> extends RayFunc {

  MultipleReturns4<R0, R1, R2, R3> apply(T0 t0) throws Throwable;

}
