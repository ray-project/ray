package org.ray.streaming.api.partition;

import org.ray.streaming.api.function.Function;

/**
 * Interface of the partitioning strategy.
 *
 * @param <T> Type of the input data.
 */
@FunctionalInterface
public interface Partition<T> extends Function {

  /**
   * Given a record and downstream partitions, determine which partition(s) should receive the
   * record.
   *
   * @param record       The record.
   * @param numPartition num of partitions
   * @return IDs of the downstream partitions that should receive the record.
   */
  int[] partition(T record, int numPartition);

}
