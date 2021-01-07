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

package io.ray.streaming.state.keystate.state.proxy;

import io.ray.streaming.state.KeyValueState;
import io.ray.streaming.state.backend.AbstractKeyStateBackend;
import io.ray.streaming.state.keystate.desc.ListStateDescriptor;
import io.ray.streaming.state.keystate.state.ListState;
import io.ray.streaming.state.keystate.state.impl.ListStateImpl;
import io.ray.streaming.state.keystate.state.impl.OperatorStateImpl;
import io.ray.streaming.state.strategy.StateStoreManagerProxy;
import java.util.List;

/** This class defines ListState Wrapper, connecting state and backend. */
public class ListStateStoreManagerProxy<T> extends StateStoreManagerProxy<List<T>>
    implements KeyValueState<String, List<T>> {

  private final ListState<T> listState;

  public ListStateStoreManagerProxy(
      AbstractKeyStateBackend keyStateBackend, ListStateDescriptor<T> stateDescriptor) {
    super(keyStateBackend, stateDescriptor);
    if (stateDescriptor.isOperatorList()) {
      this.listState = new OperatorStateImpl<>(stateDescriptor, keyStateBackend);
    } else {
      this.listState = new ListStateImpl<>(stateDescriptor, keyStateBackend);
    }
  }

  public ListState<T> getListState() {
    return this.listState;
  }
}
