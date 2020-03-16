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

package org.ray.streaming.state.backend.impl;

import java.util.Map;
import org.ray.streaming.state.backend.AbstractStateBackend;
import org.ray.streaming.state.serde.IKMapStoreSerDe;
import org.ray.streaming.state.store.IKMapStore;
import org.ray.streaming.state.store.IKVStore;
import org.ray.streaming.state.store.impl.MemoryKMapStore;
import org.ray.streaming.state.store.impl.MemoryKVStore;

/**
 * MemoryStateBackend. Supporting memory store.
 */
public class MemoryStateBackend extends AbstractStateBackend {

  public MemoryStateBackend(Map<String, String> config) {
    super(config);
  }

  @Override
  public <K, V> IKVStore<K, V> getKeyValueStore(String tableName) {
    return new MemoryKVStore<>();
  }

  @Override
  public <K, S, T> IKMapStore<K, S, T> getKeyMapStore(String tableName) {
    return new MemoryKMapStore<>();
  }

  @Override
  public <K, S, T> IKMapStore<K, S, T> getKeyMapStoreWithSerde(String tableName,
                                                               IKMapStoreSerDe ikMapStoreSerDe) {
    return new MemoryKMapStore<>();
  }

}
