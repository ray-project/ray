package io.ray.streaming.api.collector;

/**
 * The collector that collects data from an upstream operator, and emits data to downstream
 * operators.
 *
 * @param <T> Type of the data to collect.
 */
public interface Collector<T> {

  void collect(T value);

}
