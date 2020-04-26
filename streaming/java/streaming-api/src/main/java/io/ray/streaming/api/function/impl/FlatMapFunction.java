package io.ray.streaming.api.function.impl;

import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.function.Function;

/**
 * Interface of flat-map functions.
 *
 * @param <T> Type of the input data.
 * @param <R> Type of the output data.
 */
@FunctionalInterface
public interface FlatMapFunction<T, R> extends Function {

  void flatMap(T value, Collector<R> collector);
}
