package org.ray.api.funcs;

import org.apache.commons.lang3.SerializationUtils;
import org.ray.api.internal.RayFunc;
import org.ray.api.returns.MultipleReturns2;

@FunctionalInterface
public interface RayFunc_3_2<T0, T1, T2, R0, R1> extends RayFunc {

  MultipleReturns2<R0, R1> apply(T0 t0, T1 t1, T2 t2) throws Throwable;

}
