package org.ray.streaming.api.partition.impl;

import org.ray.streaming.api.partition.Partition;

/**
 * Broadcast the record to all downstream tasks.
 */
public class BroadcastPartition<T> implements Partition<T> {

  public BroadcastPartition() {
  }

  @Override
  public int[] partition(T value, int[] taskIds) {
    return taskIds;
  }
}
