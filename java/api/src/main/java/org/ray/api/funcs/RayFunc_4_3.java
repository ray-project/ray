package org.ray.api.funcs;

import org.apache.commons.lang3.SerializationUtils;
import org.ray.api.internal.RayFunc;
import org.ray.api.returns.MultipleReturns3;

@FunctionalInterface
public interface RayFunc_4_3<T0, T1, T2, T3, R0, R1, R2> extends RayFunc {

  MultipleReturns3<R0, R1, R2> apply(T0 t0, T1 t1, T2 t2, T3 t3) throws Throwable;

}
