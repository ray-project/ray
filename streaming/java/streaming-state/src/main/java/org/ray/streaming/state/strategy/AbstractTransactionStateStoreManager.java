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

package org.ray.streaming.state.strategy;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.ray.streaming.state.ITransactionStateStoreManager;
import org.ray.streaming.state.StateException;
import org.ray.streaming.state.StorageRecord;
import org.ray.streaming.state.serde.SerDeHelper;
import org.ray.streaming.state.store.IKVStore;

/**
 * This class defines the StoreManager Abstract class.
 * We use three layer to store the state, frontStore, middleStore and kvStore(remote).
 */
public abstract class AbstractTransactionStateStoreManager<V> implements
    ITransactionStateStoreManager {

  /**
   * read-write
   */
  protected Map<String, StorageRecord<V>> frontStore = new ConcurrentHashMap<>();

  /**
   * remote-storage
   */
  protected IKVStore<String, Map<Long, byte[]>> kvStore;

  /**
   * read-only
   */
  protected Map<Long, Map<String, byte[]>> middleStore = new ConcurrentHashMap<>();
  protected int keyGroupIndex = -1;

  public AbstractTransactionStateStoreManager(IKVStore<String, Map<Long, byte[]>> backStore) {
    kvStore = backStore;
  }

  public byte[] toByte(StorageRecord storageRecord) {
    return SerDeHelper.object2Byte(storageRecord);
  }

  public StorageRecord<V> toStorageRecord(byte[] data) {
    return (StorageRecord<V>) SerDeHelper.byte2Object(data);
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
