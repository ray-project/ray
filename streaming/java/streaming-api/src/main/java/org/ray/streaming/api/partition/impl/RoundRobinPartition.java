package org.ray.streaming.api.partition.impl;

import org.ray.streaming.api.partition.Partition;

/**
 * Partition record to downstream tasks in a round-robin matter.
 *
 * @param <T> Type of the input record.
 */
public class RoundRobinPartition<T> implements Partition<T> {

  private int seq;

  public RoundRobinPartition() {
    this.seq = 0;
  }

  @Override
  public int[] partition(T value, int[] taskIds) {
    int length = taskIds.length;
    int taskId = taskIds[seq++ % length];
    return new int[]{taskId};
  }
}
