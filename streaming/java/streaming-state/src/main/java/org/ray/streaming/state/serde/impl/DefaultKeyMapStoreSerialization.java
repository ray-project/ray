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

package org.ray.streaming.state.serde.impl;

import org.ray.streaming.state.serde.KeyMapStoreSerialization;
import org.ray.streaming.state.serde.SerializationHelper;

/**
 * Default Key Map Serialization and Deserialization.
 */
public class DefaultKeyMapStoreSerialization<K, S, T> extends AbstractSerialization implements
    KeyMapStoreSerialization<K, S, T> {

  @Override
  public byte[] serKey(K key) {
    String keyWithPrefix = generateRowKeyPrefix(key.toString());
    return keyWithPrefix.getBytes();
  }

  @Override
  public byte[] serUKey(S uk) {
    return SerializationHelper.object2Byte(uk);
  }

  @Override
  public S deSerUKey(byte[] ukArray) {
    return (S) SerializationHelper.byte2Object(ukArray);
  }

  @Override
  public byte[] serUValue(T uv) {
    return SerializationHelper.object2Byte(uv);
  }

  @Override
  public T deSerUValue(byte[] uvArray) {
    return (T) SerializationHelper.byte2Object(uvArray);
  }
}
