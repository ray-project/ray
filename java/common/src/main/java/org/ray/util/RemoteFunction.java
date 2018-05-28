package org.ray.util;

import java.io.Serializable;
import java.util.function.Function;

@FunctionalInterface
public interface RemoteFunction<I, O> extends Function<I, O>, Serializable {

}
