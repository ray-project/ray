package org.ray.api.funcs;

import java.util.Collection;
import java.util.Map;
import org.apache.commons.lang3.SerializationUtils;
import org.ray.api.internal.RayFunc;

@FunctionalInterface
public interface RayFunc_0_n<R, RIDT> extends RayFunc {

  Map<RIDT, R> apply(Collection<RIDT> returnids) throws Throwable;

}
