/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.ray.streaming.state.strategy;

import io.ray.streaming.state.StateException;
import io.ray.streaming.state.StateStoreManager;
import io.ray.streaming.state.StorageRecord;
import io.ray.streaming.state.serialization.Serializer;
import io.ray.streaming.state.store.KeyValueStore;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class defines the StoreManager Abstract class. We use three layer to store the state,
 * frontStore, middleStore and keyValueStore(remote).
 */
public abstract class AbstractStateStoreManager<V> implements StateStoreManager {

  /** read-write */
  protected Map<String, StorageRecord<V>> frontStore = new ConcurrentHashMap<>();

  /** remote-storage */
  protected KeyValueStore<String, Map<Long, byte[]>> kvStore;

  /** read-only */
  protected Map<Long, Map<String, byte[]>> middleStore = new ConcurrentHashMap<>();

  protected int keyGroupIndex = -1;

  public AbstractStateStoreManager(KeyValueStore<String, Map<Long, byte[]>> backStore) {
    kvStore = backStore;
  }

  public byte[] toBytes(StorageRecord storageRecord) {
    return Serializer.object2Bytes(storageRecord);
  }

  public StorageRecord<V> toStorageRecord(byte[] data) {
    return (StorageRecord<V>) Serializer.bytes2Object(data);
  }

  public abstract V get(long checkpointId, String key);

  public void put(long checkpointId, String k, V v) {
    frontStore.put(k, new StorageRecord<>(checkpointId, v));
  }

  @Override
  public void ackCommit(long checkpointId, long timeStamp) {
    ackCommit(checkpointId);
  }

  public abstract void ackCommit(long checkpointId);

  public void setKeyGroupIndex(int keyGroupIndex) {
    this.keyGroupIndex = keyGroupIndex;
  }

  public void close() {
    frontStore.clear();
    middleStore.clear();
    if (kvStore != null) {
      kvStore.clearCache();
      try {
        kvStore.close();
      } catch (IOException e) {
        throw new StateException(e);
      }
    }
  }
}
