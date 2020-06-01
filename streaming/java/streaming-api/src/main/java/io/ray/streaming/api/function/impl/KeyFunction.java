package io.ray.streaming.api.function.impl;

import io.ray.streaming.api.function.Function;

/**
 * Interface of key-by functions.
 *
 * @param <T> Type of the input data.
 * @param <K> Type of the key-by field.
 */
@FunctionalInterface
public interface KeyFunction<T, K> extends Function {

  K keyBy(T value);
}
