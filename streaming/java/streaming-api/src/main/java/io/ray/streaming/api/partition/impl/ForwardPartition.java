package io.ray.streaming.api.partition.impl;

import io.ray.streaming.api.partition.Partition;

/**
 * Default partition for operator if the operator can be chained with succeeding operators.
 * Partition will be set to {@link RoundRobinPartition} if the operator can't be chiained with
 * succeeding operators.
 *
 * @param <T> Type of the input record.
 */
public class ForwardPartition<T> implements Partition<T> {
  private int[] partitions = new int[] {0};

  @Override
  public int[] partition(T record, int numPartition) {
    return partitions;
  }
}
