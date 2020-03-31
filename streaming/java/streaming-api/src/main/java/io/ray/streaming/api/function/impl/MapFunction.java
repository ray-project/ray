package io.ray.streaming.api.function.impl;

import io.ray.streaming.api.function.Function;

/**
 * Interface of map functions.
 *
 * @param <T> type of the input data.
 * @param <R> type of the output data.
 */
@FunctionalInterface
public interface MapFunction<T, R> extends Function {

  R map(T value);
}
