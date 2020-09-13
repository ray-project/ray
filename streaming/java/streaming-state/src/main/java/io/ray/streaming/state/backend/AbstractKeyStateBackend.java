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

package io.ray.streaming.state.backend;

import io.ray.streaming.state.StateStoreManager;
import io.ray.streaming.state.keystate.desc.AbstractStateDescriptor;
import io.ray.streaming.state.keystate.desc.AbstractStateDescriptor.StateType;
import io.ray.streaming.state.keystate.state.proxy.ListStateStoreManagerProxy;
import io.ray.streaming.state.keystate.state.proxy.MapStateStoreManagerProxy;
import io.ray.streaming.state.keystate.state.proxy.ValueStateStoreManagerProxy;
import io.ray.streaming.state.store.KeyMapStore;
import io.ray.streaming.state.store.KeyValueStore;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transaction support primitive operations like finish, commit, ackcommit and rollback.
 * <p>
 * State value modification is not thread safe! By default, every processing thread has its own
 * space to handle state.
 */
public abstract class AbstractKeyStateBackend implements StateStoreManager {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractKeyStateBackend.class);

  protected long currentCheckpointId;
  protected Object currentKey;
  protected int keyGroupIndex = -1;
  protected Map<String, ValueStateStoreManagerProxy> valueManagerProxyHashMap = new HashMap<>();
  protected Map<String, ListStateStoreManagerProxy> listManagerProxyHashMap = new HashMap<>();
  protected Map<String, MapStateStoreManagerProxy> mapManagerProxyHashMap = new HashMap<>();
  protected Set<String> descNamespace;

  /**
   * tablename, KeyValueStore key, checkpointId, content
   */
  protected Map<String, KeyValueStore<String, Map<Long, byte[]>>> backStorageCache;
  private AbstractStateBackend backend;

  public AbstractKeyStateBackend(AbstractStateBackend backend) {
    this.backStorageCache = new HashMap<>();
    this.backend = backend;
    this.descNamespace = new HashSet<>();
  }

  public <K, T> void put(AbstractStateDescriptor descriptor, K key, T value) {
    String desc = descriptor.getIdentify();
    if (descriptor.getStateType() == StateType.VALUE) {
      if (this.valueManagerProxyHashMap.containsKey(desc)) {
        valueManagerProxyHashMap.get(desc).put((String) key, value);
      }
    } else if (descriptor.getStateType() == StateType.LIST) {
      if (this.listManagerProxyHashMap.containsKey(desc)) {
        listManagerProxyHashMap.get(desc).put((String) key, value);
      }
    } else if (descriptor.getStateType() == StateType.MAP) {
      if (this.mapManagerProxyHashMap.containsKey(desc)) {
        mapManagerProxyHashMap.get(desc).put((String) key, value);
      }
    }
  }

  public <K, T> T get(AbstractStateDescriptor descriptor, K key) {
    String desc = descriptor.getIdentify();
    if (descriptor.getStateType() == StateType.VALUE) {
      if (this.valueManagerProxyHashMap.containsKey(desc)) {
        return (T) valueManagerProxyHashMap.get(desc).get((String) key);
      }
    } else if (descriptor.getStateType() == StateType.LIST) {
      if (this.listManagerProxyHashMap.containsKey(desc)) {
        return (T) listManagerProxyHashMap.get(desc).get((String) key);
      }
    } else if (descriptor.getStateType() == StateType.MAP) {
      if (this.mapManagerProxyHashMap.containsKey(desc)) {
        return (T) mapManagerProxyHashMap.get(desc).get((String) key);
      }
    }
    return null;
  }

  @Override
  public void finish(long checkpointId) {
    for (Entry<String, ValueStateStoreManagerProxy> entry : valueManagerProxyHashMap.entrySet()) {
      entry.getValue().finish(checkpointId);
    }
    for (Entry<String, ListStateStoreManagerProxy> entry : listManagerProxyHashMap.entrySet()) {
      entry.getValue().finish(checkpointId);
    }
    for (Entry<String, MapStateStoreManagerProxy> entry : mapManagerProxyHashMap.entrySet()) {
      entry.getValue().finish(checkpointId);
    }
  }

  @Override
  public void commit(long checkpointId) {
    for (Entry<String, ValueStateStoreManagerProxy> entry : valueManagerProxyHashMap.entrySet()) {
      entry.getValue().commit(checkpointId);
    }
    for (Entry<String, ListStateStoreManagerProxy> entry : listManagerProxyHashMap.entrySet()) {
      entry.getValue().commit(checkpointId);
    }
    for (Entry<String, MapStateStoreManagerProxy> entry : mapManagerProxyHashMap.entrySet()) {
      entry.getValue().commit(checkpointId);
    }
  }

  @Override
  public void ackCommit(long checkpointId, long timeStamp) {
    for (Entry<String, ValueStateStoreManagerProxy> entry : valueManagerProxyHashMap.entrySet()) {
      entry.getValue().ackCommit(checkpointId, timeStamp);
    }
    for (Entry<String, ListStateStoreManagerProxy> entry : listManagerProxyHashMap.entrySet()) {
      entry.getValue().ackCommit(checkpointId, timeStamp);
    }
    for (Entry<String, MapStateStoreManagerProxy> entry : mapManagerProxyHashMap.entrySet()) {
      entry.getValue().ackCommit(checkpointId, timeStamp);
    }
  }

  @Override
  public void rollBack(long checkpointId) {
    for (Entry<String, ValueStateStoreManagerProxy> entry : valueManagerProxyHashMap.entrySet()) {
      LOG.warn("backend rollback:{},{}", entry.getKey(), checkpointId);
      entry.getValue().rollBack(checkpointId);
    }
    for (Entry<String, ListStateStoreManagerProxy> entry : listManagerProxyHashMap.entrySet()) {
      LOG.warn("backend rollback:{},{}", entry.getKey(), checkpointId);
      entry.getValue().rollBack(checkpointId);
    }
    for (Entry<String, MapStateStoreManagerProxy> entry : mapManagerProxyHashMap.entrySet()) {
      LOG.warn("backend rollback:{},{}", entry.getKey(), checkpointId);
      entry.getValue().rollBack(checkpointId);
    }
  }

  public KeyValueStore<String, Map<Long, byte[]>> getBackStorage(String tableName) {
    if (this.backStorageCache.containsKey(tableName)) {
      return this.backStorageCache.get(tableName);
    } else {
      KeyMapStore<String, Long, byte[]> ikvStore = this.backend.getKeyMapStore(tableName);
      this.backStorageCache.put(tableName, ikvStore);
      return ikvStore;
    }
  }

  public KeyValueStore<String, Map<Long, byte[]>> getBackStorage(
      AbstractStateDescriptor stateDescriptor) {
    String tableName = this.backend.getTableName(stateDescriptor);
    return getBackStorage(tableName);
  }

  public StateStrategy getStateStrategy() {
    return this.backend.getStateStrategy();
  }

  public BackendType getBackendType() {
    return this.backend.getBackendType();
  }

  public Object getCurrentKey() {
    return this.currentKey;
  }

  public abstract void setCurrentKey(Object currentKey);

  public long getCheckpointId() {
    return this.currentCheckpointId;
  }

  public void setCheckpointId(long checkpointId) {
    this.currentCheckpointId = checkpointId;
  }

  public void setContext(long checkpointId, Object currentKey) {
    setCheckpointId(checkpointId);
    setCurrentKey(currentKey);
  }

  public AbstractStateBackend getBackend() {
    return backend;
  }

  public int getKeyGroupIndex() {
    return this.keyGroupIndex;
  }

  public void setKeyGroupIndex(int keyGroupIndex) {
    this.keyGroupIndex = keyGroupIndex;
  }
}
