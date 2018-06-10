package org.ray.api.funcs;

import java.util.Collection;
import java.util.Map;
import org.apache.commons.lang3.SerializationUtils;
import org.ray.api.internal.RayFunc;

@FunctionalInterface
public interface RayFunc_2_n<T0, T1, R, RIDT> extends RayFunc {

  Map<RIDT, R> apply(Collection<RIDT> returnids, T0 t0, T1 t1) throws Throwable;

}
