package org.ray.api.funcs;

import org.apache.commons.lang3.SerializationUtils;
import org.ray.api.internal.RayFunc;
import org.ray.api.returns.MultipleReturns3;

@FunctionalInterface
public interface RayFunc_1_3<T0, R0, R1, R2> extends RayFunc {

  MultipleReturns3<R0, R1, R2> apply(T0 t0) throws Throwable;

}
