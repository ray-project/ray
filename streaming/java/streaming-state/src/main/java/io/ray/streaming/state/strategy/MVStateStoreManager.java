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
import io.ray.streaming.state.StorageRecord;
import io.ray.streaming.state.store.KeyValueStore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * This class define the multi-version store strategy, which leverages external storage's mvcc.
 */
public class MVStateStoreManager<V> extends AbstractStateStoreManager<V> {

  public MVStateStoreManager(KeyValueStore<String, Map<Long, byte[]>> kvStore) {
    super(kvStore);
  }

  @Override
  public void finish(long checkpointId) {
    Map<String, byte[]> currentStateRecords = new HashMap<>();
    for (Entry<String, StorageRecord<V>> entry : frontStore.entrySet()) {
      currentStateRecords.put(entry.getKey(), toBytes(entry.getValue()));
    }

    middleStore.put(checkpointId, currentStateRecords);
    frontStore.clear();
  }

  @Override
  public void commit(long checkpointId) {
    // write to external storage
    List<Long> checkpointIds = new ArrayList<>(middleStore.keySet());
    Collections.sort(checkpointIds);

    for (int i = checkpointIds.size() - 1; i >= 0; i--) {
      long commitBatchId = checkpointIds.get(i);
      if (commitBatchId > checkpointId) {
        continue;
      }

      Map<String, byte[]> commitRecords = middleStore.get(commitBatchId);

      try {
        for (Entry<String, byte[]> entry : commitRecords.entrySet()) {

          Map<Long, byte[]> remoteData = this.kvStore.get(entry.getKey());
          if (remoteData == null) {
            remoteData = new HashMap<>();
          }

          remoteData.put(commitBatchId, entry.getValue());

          this.kvStore.put(entry.getKey(), remoteData);
        }
        this.kvStore.flush();
      } catch (Exception e) {
        throw new StateException(e);
      }
    }
  }

  @Override
  public void rollBack(long checkpointId) {
    this.frontStore.clear();
    this.middleStore.clear();
    this.kvStore.clearCache();
  }

  @Override
  public V get(long checkpointId, String key) {
    StorageRecord<V> valueArray = frontStore.get(key);
    if (valueArray != null) {
      return valueArray.getValue();
    } else {
      List<Long> checkpointIds = new ArrayList<>(middleStore.keySet());
      Collections.sort(checkpointIds);

      for (int i = checkpointIds.size() - 1; i >= 0; i--) {
        if (checkpointIds.get(i) > checkpointId) {
          continue;
        }

        Map<String, byte[]> records = middleStore.get(checkpointIds.get(i));
        if (records != null) {
          if (records.containsKey(key)) {
            byte[] bytes = records.get(key);
            return toStorageRecord(bytes).getValue();
          }
        }
      }

      // get from external storage
      try {
        Map<Long, byte[]> remoteData = this.kvStore.get(key);
        if (remoteData != null) {
          checkpointIds = new ArrayList<>(remoteData.keySet());
          Collections.sort(checkpointIds);

          for (int i = checkpointIds.size() - 1; i >= 0; i--) {
            if (checkpointIds.get(i) > checkpointId) {
              continue;
            }

            byte[] bytes = remoteData.get(checkpointIds.get(i));
            return toStorageRecord(bytes).getValue();
          }
        }
      } catch (Exception e) {
        throw new StateException(e);
      }
    }
    return null;
  }

  @Override
  public void put(long checkpointId, String k, V v) {
    frontStore.put(k, new StorageRecord<>(checkpointId, v));
  }

  @Override
  public void ackCommit(long checkpointId) {
    List<Long> checkpointIds = new ArrayList<>(middleStore.keySet());
    Collections.sort(checkpointIds);

    for (int i = checkpointIds.size() - 1; i >= 0; i--) {
      long commitBatchId = checkpointIds.get(i);
      if (commitBatchId > checkpointId) {
        continue;
      }

      this.middleStore.remove(commitBatchId);
    }
  }

}
