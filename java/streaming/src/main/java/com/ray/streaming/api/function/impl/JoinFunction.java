package com.ray.streaming.api.function.impl;

import com.ray.streaming.api.function.Function;
import java.io.Serializable;

/**
 * Interface of join functions.
 *
 * @param <T> Type of the left input data.
 * @param <O> Type of the right input data.
 * @param <R> Type of the output data.
 */
@FunctionalInterface
public interface JoinFunction<T, O, R> extends Function {

  R join(T left, O right);
}
