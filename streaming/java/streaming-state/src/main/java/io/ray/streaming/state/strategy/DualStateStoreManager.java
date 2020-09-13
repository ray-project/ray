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

import com.google.common.primitives.Longs;
import io.ray.streaming.state.StateException;
import io.ray.streaming.state.StorageRecord;
import io.ray.streaming.state.store.KeyValueStore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class define the checkpoint store strategy, which saves two-version data once.
 */
public class DualStateStoreManager<V> extends AbstractStateStoreManager<V> {

  private static final Logger LOG = LoggerFactory.getLogger(DualStateStoreManager.class);

  public DualStateStoreManager(KeyValueStore<String, Map<Long, byte[]>> backStore) {
    super(backStore);
  }

  @Override
  public void finish(long checkpointId) {
    LOG.info("do finish checkpointId:{}", checkpointId);
    Map<String, byte[]> cpStore = new HashMap<>();
    for (Entry<String, StorageRecord<V>> entry : frontStore.entrySet()) {
      String key = entry.getKey();
      StorageRecord<V> value = entry.getValue();
      cpStore.put(key, toBytes(value));
    }
    middleStore.put(checkpointId, cpStore);
    frontStore.clear();
  }

  @Override
  public void commit(long checkpointId) {
    try {
      LOG.info("do commit checkpointId:{}", checkpointId);
      Map<String, byte[]> cpStore = middleStore.get(checkpointId);
      if (cpStore == null) {
        throw new StateException("why cp store is null");
      }
      for (Entry<String, byte[]> entry : cpStore.entrySet()) {
        String key = entry.getKey();
        byte[] value = entry.getValue();

        /**
         * 2 is specific key in kv store and indicates that new value
         * should be stored with this key after overwriting old value in key 1. i.e.
         *
         *      -2     -1     1        2
         * k1   6      5      a        b
         * k2   9      7      d        e
         *
         * k1's value for checkpoint 5 is a, and b for checkpoint 6.
         */
        Map<Long, byte[]> remoteData = super.kvStore.get(key);
        if (remoteData == null || remoteData.size() == 0) {
          remoteData = new HashMap<>();
          remoteData.put(2L, value);
          remoteData.put(-2L, Longs.toByteArray(checkpointId));
        } else {
          long oldBatchId = Longs.fromByteArray(remoteData.get(-2L));
          if (oldBatchId < checkpointId) {
            //move the old data
            remoteData.put(1L, remoteData.get(2L));
            remoteData.put(-1L, remoteData.get(-2L));
          }

          //put the new data here
          remoteData.put(2L, value);
          remoteData.put(-2L, Longs.toByteArray(checkpointId));
        }
        super.kvStore.put(key, remoteData);
      }
      super.kvStore.flush();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new StateException(e);
    }
  }

  @Override
  public void rollBack(long checkpointId) {
    LOG.info("do rollBack checkpointId:{}", checkpointId);
    this.frontStore.clear();
    this.middleStore.clear();
    this.kvStore.clearCache();
  }

  @Override
  public V get(long checkpointId, String key) {
    // get from current cp cache
    StorageRecord<V> storageRecord = frontStore.get(key);
    if (storageRecord != null) {
      return storageRecord.getValue();
    }

    //get from not commit cp info
    List<Long> checkpointIds = new ArrayList<>(middleStore.keySet());
    Collections.sort(checkpointIds);
    for (int i = checkpointIds.size() - 1; i >= 0; i--) {
      Map<String, byte[]> cpStore = middleStore.get(checkpointIds.get(i));
      if (cpStore != null) {
        if (cpStore.containsKey(key)) {
          byte[] cpData = cpStore.get(key);
          storageRecord = toStorageRecord(cpData);
          return storageRecord.getValue();
        }
      }
    }

    try {
      Map<Long, byte[]> remoteData = super.kvStore.get(key);
      if (remoteData != null) {
        for (Entry<Long, byte[]> entry : remoteData.entrySet()) {
          if (entry.getKey() > 0) {
            StorageRecord<V> tmp = toStorageRecord(entry.getValue());
            if (tmp.getCheckpointId() < checkpointId) {
              if (storageRecord == null) {
                storageRecord = tmp;
              } else if (storageRecord.getCheckpointId() < tmp.getCheckpointId()) {
                storageRecord = tmp;
              }
            }
          }
        }
        if (storageRecord != null) {
          return storageRecord.getValue();
        }
      }
    } catch (Exception e) {
      LOG.error("get checkpointId:" + checkpointId + " key:" + key, e);
      throw new StateException(e);
    }
    return null;
  }

  @Override
  public void ackCommit(long checkpointId) {
    LOG.info("do ackCommit checkpointId:{}", checkpointId);
    middleStore.remove(checkpointId);
  }
}
