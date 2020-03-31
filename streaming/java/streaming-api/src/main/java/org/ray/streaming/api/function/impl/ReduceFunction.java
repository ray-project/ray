package io.ray.streaming.api.function.impl;

import io.ray.streaming.api.function.Function;

/**
 * Interface of reduce functions.
 *
 * @param <T> Type of the input data.
 */
@FunctionalInterface
public interface ReduceFunction<T> extends Function {

  T reduce(T oldValue, T newValue);
}
