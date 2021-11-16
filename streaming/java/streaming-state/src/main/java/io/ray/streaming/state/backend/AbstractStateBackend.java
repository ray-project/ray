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

import static io.ray.streaming.state.config.ConfigKey.DELIMITER;

import io.ray.streaming.state.config.ConfigKey;
import io.ray.streaming.state.keystate.desc.AbstractStateDescriptor;
import io.ray.streaming.state.serialization.KeyMapStoreSerializer;
import io.ray.streaming.state.store.KeyMapStore;
import io.ray.streaming.state.store.KeyValueStore;
import java.io.Serializable;
import java.util.Map;

/** This class is the abstract class for different kinds of StateBackend. */
public abstract class AbstractStateBackend implements Serializable {

  protected final Map<String, String> config;
  protected final StateStrategy stateStrategy;

  protected final BackendType backendType;
  protected int keyGroupIndex = -1;

  protected AbstractStateBackend(Map<String, String> config) {
    this.stateStrategy = StateStrategy.getEnum(ConfigKey.getStateStrategyEnum(config));
    this.backendType = BackendType.getEnum(ConfigKey.getBackendType(config));
    this.config = config;
  }

  public abstract <K, V> KeyValueStore<K, V> getKeyValueStore(String tableName);

  public abstract <K, S, T> KeyMapStore<K, S, T> getKeyMapStore(String tableName);

  public abstract <K, S, T> KeyMapStore<K, S, T> getKeyMapStore(
      String tableName, KeyMapStoreSerializer keyMapStoreSerializer);

  public BackendType getBackendType() {
    return backendType;
  }

  public StateStrategy getStateStrategy() {
    return stateStrategy;
  }

  public String getTableName(AbstractStateDescriptor stateDescriptor) {
    return stateDescriptor.getTableName();
  }

  public String getStateKey(String descName, String currentKey) {
    return descName + DELIMITER + currentKey;
  }

  public void setKeyGroupIndex(int keyGroupIndex) {
    this.keyGroupIndex = keyGroupIndex;
  }
}
