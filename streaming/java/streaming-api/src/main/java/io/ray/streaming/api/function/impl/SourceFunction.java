package io.ray.streaming.api.function.impl;

import io.ray.streaming.api.function.Function;

/**
 * Interface of Source functions.
 *
 * @param <T> Type of the data output by the source.
 */
public interface SourceFunction<T> extends Function {

  void init(int parallelism, int index);

  void fetch(SourceContext<T> ctx) throws Exception;

  void close();

  interface SourceContext<T> {

    void collect(T element) throws Exception;
  }
}
