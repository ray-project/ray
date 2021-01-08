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

package io.ray.streaming.state.keystate.state;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/** MapState interface. */
public interface MapState<K, V> extends UnaryState<Map<K, V>> {

  /**
   * Returns the current value associated with the given key.
   *
   * @param key The key of the mapping Returns The value of the mapping with the given key
   */
  V get(K key);

  /**
   * Associates a new value with the given key.
   *
   * @param key The key of the mapping
   * @param value The new value of the mapping
   */
  void put(K key, V value);

  /**
   * Resets the state value.
   *
   * @param map The mappings for reset in this state
   */
  void update(Map<K, V> map);

  /**
   * Copies all of the mappings from the given map into the state.
   *
   * @param map The mappings to be stored in this state
   */
  void putAll(Map<K, V> map);

  /**
   * Deletes the mapping of the given key.
   *
   * @param key The key of the mapping
   */
  void remove(K key);

  /**
   * Returns whether there exists the given mapping.
   *
   * @param key The key of the mapping Returns True if there exists a mapping whose key equals to
   *     the given key
   */
  default boolean contains(K key) {
    return get().containsKey(key);
  }

  /**
   * Returns all the mappings in the state
   *
   * <p>Returns An iterable view of all the key-value pairs in the state.
   */
  default Iterable<Entry<K, V>> entries() {
    return get().entrySet();
  }

  /**
   * Returns all the keys in the state
   *
   * <p>Returns An iterable view of all the keys in the state.
   */
  default Iterable<K> keys() {
    return get().keySet();
  }

  /**
   * Returns all the values in the state.
   *
   * <p>Returns An iterable view of all the values in the state.
   */
  default Iterable<V> values() {
    return get().values();
  }

  /**
   * Iterates over all the mappings in the state.
   *
   * <p>Returns An iterator over all the mappings in the state
   */
  default Iterator<Entry<K, V>> iterator() {
    return get().entrySet().iterator();
  }
}
