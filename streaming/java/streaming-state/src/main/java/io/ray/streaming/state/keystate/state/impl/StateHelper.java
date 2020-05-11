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

import com.google.common.base.Preconditions;
import io.ray.streaming.state.backend.AbstractKeyStateBackend;
import io.ray.streaming.state.keystate.desc.AbstractStateDescriptor;

/**
 * State Helper Class.
 */
public class StateHelper<T> {

  private final AbstractKeyStateBackend backend;
  private final AbstractStateDescriptor descriptor;

  public StateHelper(AbstractKeyStateBackend backend, AbstractStateDescriptor descriptor) {
    this.backend = backend;
    this.descriptor = descriptor;
  }

  protected String getStateKey(String descName) {
    Preconditions.checkNotNull(backend, "KeyedBackend must not be null");
    Preconditions.checkNotNull(backend.getCurrentKey(), "currentKey must not be null");
    return this.backend.getBackend().getStateKey(descName, backend.getCurrentKey().toString());
  }

  public void put(T value, String key) {
    backend.put(descriptor, key, value);
  }

  public void put(T value) {
    put(value, getStateKey(descriptor.getIdentify()));
  }

  public T get(String key) {
    return backend.get(descriptor, key);
  }

  public T get() {
    return get(getStateKey(descriptor.getIdentify()));
  }

  public void setCurrentKey(Object currentKey) {
    Preconditions.checkNotNull(backend, "KeyedBackend must not be null");
    this.backend.setCurrentKey(currentKey);
  }

  public void setKeyGroupIndex(int keyGroupIndex) {
    this.backend.setKeyGroupIndex(keyGroupIndex);
  }

  public void resetKeyGroupIndex() {
    this.backend.setKeyGroupIndex(-1);
  }

  public AbstractStateDescriptor getDescriptor() {
    return descriptor;
  }

  public AbstractKeyStateBackend getBackend() {
    return backend;
  }
}
