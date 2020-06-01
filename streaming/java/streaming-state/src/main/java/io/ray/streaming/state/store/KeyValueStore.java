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

package io.ray.streaming.state.store;

import java.io.IOException;
import java.io.Serializable;

/**
 * Key Value Store interface.
 */
public interface KeyValueStore<K, V> extends Serializable {

  /**
   * put key value into store.
   */
  void put(K key, V value) throws IOException;

  /**
   * get value from store.
   */
  V get(K key) throws IOException;

  /**
   * remove key in the store.
   */
  void remove(K key) throws IOException;

  /**
   * flush to disk.
   */
  void flush() throws IOException;

  /**
   * clear all cache.
   */
  void clearCache();

  /**
   * close the store.
   */
  void close() throws IOException;
}
