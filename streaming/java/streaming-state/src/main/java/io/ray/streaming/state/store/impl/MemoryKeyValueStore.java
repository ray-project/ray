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

package io.ray.streaming.state.store.impl;

import com.google.common.collect.Maps;
import io.ray.streaming.state.store.KeyValueStore;
import java.io.IOException;
import java.util.Map;

/** Memory Key Value Store. */
public class MemoryKeyValueStore<K, V> implements KeyValueStore<K, V> {

  private Map<K, V> memoryStore;

  public MemoryKeyValueStore() {
    this.memoryStore = Maps.newConcurrentMap();
  }

  @Override
  public void put(K key, V value) throws IOException {
    this.memoryStore.put(key, value);
  }

  @Override
  public V get(K key) throws IOException {
    return this.memoryStore.get(key);
  }

  @Override
  public void remove(K key) throws IOException {
    this.memoryStore.remove(key);
  }

  @Override
  public void flush() throws IOException {}

  @Override
  public void clearCache() {}

  @Override
  public void close() throws IOException {
    if (memoryStore != null) {
      memoryStore.clear();
    }
  }
}
