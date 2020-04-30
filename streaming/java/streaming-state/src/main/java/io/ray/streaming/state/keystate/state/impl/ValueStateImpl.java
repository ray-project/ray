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
import io.ray.streaming.state.keystate.desc.ValueStateDescriptor;
import io.ray.streaming.state.keystate.state.ValueState;

/**
 * ValueState implementation.
 */
public class ValueStateImpl<T> implements ValueState<T> {

  private final StateHelper<T> helper;

  public ValueStateImpl(ValueStateDescriptor<T> descriptor, KeyStateBackend backend) {
    this.helper = new StateHelper<>(backend, descriptor);
  }

  @Override
  public void update(T value) {
    helper.put(value);
  }

  @Override
  public T get() {
    T value = helper.get();
    if (null == value) {
      return ((ValueStateDescriptor<T>) helper.getDescriptor()).getDefaultValue();
    } else {
      return value;
    }
  }

  /**
   * set current key of the state
   */
  @Override
  public void setCurrentKey(Object currentKey) {
    helper.setCurrentKey(currentKey);
  }
}
