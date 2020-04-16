package io.ray.streaming.api.function.impl;

import io.ray.streaming.api.function.Function;

/**
 * A filter function is a predicate applied individually to each record.
 * The predicate decides whether to keep the element, or to discard it.
 *
 * @param <T> type of the input data.
 */
@FunctionalInterface
public interface FilterFunction<T> extends Function {

  /**
   * The filter function that evaluates the predicate.
   *
   * @param value The value to be filtered.
   * @return True for values that should be retained, false for values to be filtered out.
   */
  boolean filter(T value) throws Exception;
}
