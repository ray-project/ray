package org.ray.streaming.api.partition.impl;

import org.ray.streaming.api.partition.Partition;
import org.ray.streaming.message.KeyRecord;

/**
 * Partition the record by the key.
 *
 * @param <K> Type of the partition key.
 * @param <T> Type of the input record.
 */
public class KeyPartition<K, T> implements Partition<KeyRecord<K, T>> {

  @Override
  public int[] partition(KeyRecord<K, T> keyRecord, int[] taskIds) {
    int length = taskIds.length;
    int taskId = taskIds[Math.abs(keyRecord.getKey().hashCode() % length)];
    return new int[]{taskId};
  }
}
