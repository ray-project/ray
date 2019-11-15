package org.ray.streaming.api.partition;

import org.ray.streaming.api.function.Function;

/**
 * Interface of the partitioning strategy.
 * @param <T> Type of the input data.
 */
@FunctionalInterface
public interface Partition<T> extends Function {

  /**
   * Given a record and downstream tasks, determine which task(s) should receive the record.
   *
   * @param record The record.
   * @param taskIds IDs of all downstream tasks.
   * @return IDs of the downstream tasks that should receive the record.
   */
  int[] partition(T record, int[] taskIds);

}
