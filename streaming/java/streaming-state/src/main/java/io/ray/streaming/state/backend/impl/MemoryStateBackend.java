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

package io.ray.streaming.state.backend.impl;

import io.ray.streaming.state.backend.AbstractStateBackend;
import io.ray.streaming.state.serialization.KeyMapStoreSerializer;
import io.ray.streaming.state.store.KeyMapStore;
import io.ray.streaming.state.store.KeyValueStore;
import io.ray.streaming.state.store.impl.MemoryKeyMapStore;
import io.ray.streaming.state.store.impl.MemoryKeyValueStore;
import java.util.Map;

/**
 * MemoryStateBackend. Supporting memory store.
 */
public class MemoryStateBackend extends AbstractStateBackend {

  public MemoryStateBackend(Map<String, String> config) {
    super(config);
  }

  @Override
  public <K, V> KeyValueStore<K, V> getKeyValueStore(String tableName) {
    return new MemoryKeyValueStore<>();
  }

  @Override
  public <K, S, T> KeyMapStore<K, S, T> getKeyMapStore(String tableName) {
    return new MemoryKeyMapStore<>();
  }

  @Override
  public <K, S, T> KeyMapStore<K, S, T> getKeyMapStore(
      String tableName, KeyMapStoreSerializer keyMapStoreSerializer) {
    return new MemoryKeyMapStore<>();
  }

}
