package io.ray.streaming.api.function.impl;

import io.ray.streaming.api.function.Function;

/**
 * Interface of sink functions.
 *
 * @param <T> Type of the sink data.
 */
@FunctionalInterface
public interface SinkFunction<T> extends Function {

  void sink(T value);
}
