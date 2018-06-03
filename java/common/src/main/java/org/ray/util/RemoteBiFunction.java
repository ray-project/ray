package org.ray.util;

import java.io.Serializable;
import java.util.function.BiFunction;

@FunctionalInterface
public interface RemoteBiFunction<T1, T2, O> extends BiFunction<T1, T2, O>, Serializable {

}

