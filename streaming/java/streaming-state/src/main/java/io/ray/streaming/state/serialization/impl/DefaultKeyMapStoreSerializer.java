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

import io.ray.streaming.state.serialization.KeyMapStoreSerializer;
import io.ray.streaming.state.serialization.Serializer;

/**
 * Default Key Map Serialization and Deserialization.
 */
public class DefaultKeyMapStoreSerializer<K, S, T> extends AbstractSerialization implements
    KeyMapStoreSerializer<K, S, T> {

  @Override
  public byte[] serializeKey(K key) {
    String keyWithPrefix = generateRowKeyPrefix(key.toString());
    return keyWithPrefix.getBytes();
  }

  @Override
  public byte[] serializeUKey(S uk) {
    return Serializer.object2Bytes(uk);
  }

  @Override
  public S deserializeUKey(byte[] ukArray) {
    return (S) Serializer.bytes2Object(ukArray);
  }

  @Override
  public byte[] serializeUValue(T uv) {
    return Serializer.object2Bytes(uv);
  }

  @Override
  public T deserializeUValue(byte[] uvArray) {
    return (T) Serializer.bytes2Object(uvArray);
  }
}
