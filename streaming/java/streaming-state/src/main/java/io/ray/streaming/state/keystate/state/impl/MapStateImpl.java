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

package io.ray.streaming.state.keystate.state.impl;

import io.ray.streaming.state.backend.KeyStateBackend;
import io.ray.streaming.state.keystate.desc.MapStateDescriptor;
import io.ray.streaming.state.keystate.state.MapState;
import java.util.HashMap;
import java.util.Map;

/** MapState implementation. */
public class MapStateImpl<K, V> implements MapState<K, V> {

  private final StateHelper<Map<K, V>> helper;

  public MapStateImpl(MapStateDescriptor<K, V> descriptor, KeyStateBackend backend) {
    this.helper = new StateHelper<>(backend, descriptor);
  }

  @Override
  public Map<K, V> get() {
    Map<K, V> map = helper.get();
    if (map == null) {
      map = new HashMap<>();
    }
    return map;
  }

  @Override
  public V get(K key) {
    Map<K, V> map = get();
    return map.get(key);
  }

  @Override
  public void put(K key, V value) {
    Map<K, V> map = get();

    map.put(key, value);
    helper.put(map);
  }

  @Override
  public void update(Map<K, V> map) {
    if (map == null) {
      map = new HashMap<>();
    }
    helper.put(map);
  }

  @Override
  public void putAll(Map<K, V> newMap) {
    Map<K, V> map = get();

    map.putAll(newMap);
    helper.put(map);
  }

  @Override
  public void remove(K key) {
    Map<K, V> map = get();

    map.remove(key);
    helper.put(map);
  }

  /** set current key of the state */
  @Override
  public void setCurrentKey(Object currentKey) {
    helper.setCurrentKey(currentKey);
  }
}
