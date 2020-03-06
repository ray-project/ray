package org.ray.streaming.api.partition.impl;

import java.util.stream.IntStream;
import org.ray.streaming.api.partition.Partition;

/**
 * Broadcast the record to all downstream partitions.
 */
public class BroadcastPartition<T> implements Partition<T> {
  private int[] partitions = new int[0];

  public BroadcastPartition() {
  }

  @Override
  public int[] partition(T value, int numPartition) {
    if (partitions.length != numPartition) {
      partitions = IntStream.rangeClosed(0, numPartition - 1).toArray();
    }
    return partitions;
  }

}
