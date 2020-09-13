package io.ray.streaming.api.collector;

import io.ray.streaming.message.Record;
import java.util.List;

/**
 * Combination of multiple collectors.
 *
 * @param <T> The type of output data.
 */
public class CollectionCollector<T> implements Collector<T> {

  private List<Collector> collectorList;

  public CollectionCollector(List<Collector> collectorList) {
    this.collectorList = collectorList;
  }

  @Override
  public void collect(T value) {
    for (Collector collector : collectorList) {
      collector.collect(new Record(value));
    }
  }
}
