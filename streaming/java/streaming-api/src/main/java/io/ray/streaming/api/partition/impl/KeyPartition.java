package io.ray.streaming.api.partition.impl;

import io.ray.streaming.api.partition.Partition;
import io.ray.streaming.message.KeyRecord;

/**
 * Partition the record by the key.
 *
 * @param <K> Type of the partition key.
 * @param <T> Type of the input record.
 */
public class KeyPartition<K, T> implements Partition<KeyRecord<K, T>> {
  private int[] partitions = new int[1];

  @Override
  public int[] partition(KeyRecord<K, T> keyRecord, int numPartition) {
    partitions[0] = Math.abs(keyRecord.getKey().hashCode() % numPartition);
    return partitions;
  }
}
