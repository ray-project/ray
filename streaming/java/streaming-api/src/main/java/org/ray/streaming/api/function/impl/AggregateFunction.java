package io.ray.streaming.api.function.impl;

import io.ray.streaming.api.function.Function;

/**
 * Interface of aggregate functions.
 *
 * @param <I> Type of the input data.
 * @param <A> Type of the intermediate data.
 * @param <O> Type of the output data.
 */
public interface AggregateFunction<I, A, O> extends Function {

  A createAccumulator();

  void add(I value, A accumulator);

  O getResult(A accumulator);

  A merge(A a, A b);

  void retract(A acc, I value);
}
