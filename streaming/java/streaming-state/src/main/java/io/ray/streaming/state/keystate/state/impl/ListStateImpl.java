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

import io.ray.streaming.state.backend.AbstractKeyStateBackend;
import io.ray.streaming.state.keystate.desc.ListStateDescriptor;
import io.ray.streaming.state.keystate.state.ListState;
import java.util.ArrayList;
import java.util.List;

/**
 * ListState implementation.
 */
public class ListStateImpl<V> implements ListState<V> {

  private final StateHelper<List<V>> helper;

  public ListStateImpl(ListStateDescriptor<V> descriptor, AbstractKeyStateBackend backend) {
    this.helper = new StateHelper<>(backend, descriptor);
  }

  @Override
  public List<V> get() {
    List<V> list = helper.get();
    if (list == null) {
      list = new ArrayList<>();
    }
    return list;
  }

  @Override
  public void add(V value) {
    List<V> list = helper.get();
    if (list == null) {
      list = new ArrayList<>();
    }
    list.add(value);
    helper.put(list);
  }

  @Override
  public void update(List<V> list) {
    if (list == null) {
      list = new ArrayList<>();
    }
    helper.put(list);
  }

  @Override
  public void setCurrentKey(Object currentKey) {
    helper.setCurrentKey(currentKey);
  }
}
