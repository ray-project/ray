package org.ray.api.funcs;

import java.util.List;
import org.apache.commons.lang3.SerializationUtils;
import org.ray.api.internal.RayFunc;

@FunctionalInterface
public interface RayFunc_1_n_list<T0, R> extends RayFunc {

  List<R> apply(T0 t0) throws Throwable;

}
