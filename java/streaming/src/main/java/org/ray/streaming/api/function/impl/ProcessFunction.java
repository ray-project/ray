package org.ray.streaming.api.function.impl;

import org.ray.streaming.api.function.Function;

/**
 * Interface of process functions.
 *
 * @param <T> Type of the input data.
 */
@FunctionalInterface
public interface ProcessFunction<T> extends Function {

  void process(T value);
}
