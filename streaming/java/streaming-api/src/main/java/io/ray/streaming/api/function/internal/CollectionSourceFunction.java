package io.ray.streaming.api.function.internal;

import io.ray.streaming.api.function.impl.SourceFunction;
import java.util.Collection;

/**
 * The SourceFunction that fetch data from a Java Collection object.
 *
 * @param <T> Type of the data output by the source.
 */
public class CollectionSourceFunction<T> implements SourceFunction<T> {

  private Collection<T> values;
  private boolean finished = false;

  public CollectionSourceFunction(Collection<T> values) {
    this.values = values;
  }

  @Override
  public void init(int totalParallel, int currentIndex) {}

  @Override
  public void fetch(SourceContext<T> ctx) throws Exception {
    if (finished) {
      return;
    }
    for (T value : values) {
      ctx.collect(value);
    }
    finished = true;
  }

  @Override
  public void close() {}
}
