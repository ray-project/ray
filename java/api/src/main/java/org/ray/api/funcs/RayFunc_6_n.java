package org.ray.api.funcs;

import java.util.Collection;
import java.util.Map;
import org.apache.commons.lang3.SerializationUtils;
import org.ray.api.internal.RayFunc;

@FunctionalInterface
public interface RayFunc_6_n<T0, T1, T2, T3, T4, T5, R, RIDT> extends RayFunc {

  Map<RIDT, R> apply(Collection<RIDT> returnids, T0 t0, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5)
      throws Throwable;

}
