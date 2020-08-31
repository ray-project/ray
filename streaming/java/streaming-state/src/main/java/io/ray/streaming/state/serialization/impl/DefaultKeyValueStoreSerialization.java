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

package io.ray.streaming.state.serialization.impl;

import io.ray.streaming.state.serialization.KeyValueStoreSerialization;
import io.ray.streaming.state.serialization.Serializer;

/**
 * KV Store Serialization and Deserialization.
 */
public class DefaultKeyValueStoreSerialization<K, V> extends AbstractSerialization implements
    KeyValueStoreSerialization<K, V> {

  @Override
  public byte[] serializeKey(K key) {
    String keyWithPrefix = generateRowKeyPrefix(key.toString());
    return keyWithPrefix.getBytes();
  }

  @Override
  public byte[] serializeValue(V value) {
    return Serializer.object2Bytes(value);
  }

  @Override
  public V deserializeValue(byte[] valueArray) {
    return (V) Serializer.bytes2Object(valueArray);
  }
}
