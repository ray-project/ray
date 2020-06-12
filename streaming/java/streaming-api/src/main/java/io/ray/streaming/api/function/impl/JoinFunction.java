package io.ray.streaming.api.function.impl;

import io.ray.streaming.api.function.Function;

/**
 * Interface of join functions.
 *
 * @param <L> Type of the left input data.
 * @param <R> Type of the right input data.
 * @param <O> Type of the output data.
 */
@FunctionalInterface
public interface JoinFunction<L, R, O> extends Function {

  O join(L left, R right);

}
