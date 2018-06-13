package org.ray.api.funcs;

import java.util.List;
import org.apache.commons.lang3.SerializationUtils;
import org.ray.api.internal.RayFunc;

@FunctionalInterface
public interface RayFunc_0_n_list<R> extends RayFunc {

  List<R> apply() throws Throwable;

}
