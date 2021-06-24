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
import io.ray.streaming.state.backend.KeyStateBackend;
import io.ray.streaming.state.keystate.desc.MapStateDescriptor;
import io.ray.streaming.state.keystate.state.MapState;
import io.ray.streaming.state.keystate.state.impl.MapStateImpl;
import io.ray.streaming.state.strategy.StateStoreManagerProxy;
import java.util.Map;

/** This class defines MapState Wrapper, connecting state and backend. */
public class MapStateStoreManagerProxy<K, V> extends StateStoreManagerProxy<Map<K, V>>
    implements KeyValueState<String, Map<K, V>> {

  private final MapStateImpl<K, V> mapState;

  public MapStateStoreManagerProxy(
      KeyStateBackend keyStateBackend, MapStateDescriptor<K, V> stateDescriptor) {
    super(keyStateBackend, stateDescriptor);
    this.mapState = new MapStateImpl<>(stateDescriptor, keyStateBackend);
  }

  public MapState<K, V> getMapState() {
    return this.mapState;
  }
}
