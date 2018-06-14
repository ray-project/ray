package org.ray.api.funcs;

import org.apache.commons.lang3.SerializationUtils;
import org.ray.api.internal.RayFunc;

@FunctionalInterface
public interface RayFunc_2_1<T0, T1, R0> extends RayFunc {

  R0 apply(T0 t0, T1 t1) throws Throwable;

}
