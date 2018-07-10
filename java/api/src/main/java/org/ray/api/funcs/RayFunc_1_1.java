package org.ray.api.funcs;

import org.apache.commons.lang3.SerializationUtils;
import org.ray.api.internal.RayFunc;

@FunctionalInterface
public interface RayFunc_1_1<T0, R0> extends RayFunc {

  R0 apply(T0 t0) throws Throwable;

}
