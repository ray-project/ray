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

package org.ray.streaming.state.backend;

import org.ray.streaming.state.keystate.KeyGroup;
import org.ray.streaming.state.keystate.KeyGroupAssignment;
import org.ray.streaming.state.keystate.desc.ListStateDescriptor;
import org.ray.streaming.state.keystate.desc.MapStateDescriptor;
import org.ray.streaming.state.keystate.desc.ValueStateDescriptor;
import org.ray.streaming.state.keystate.state.ListState;
import org.ray.streaming.state.keystate.state.MapState;
import org.ray.streaming.state.keystate.state.ValueState;
import org.ray.streaming.state.keystate.state.proxy.ListStateStoreManagerProxy;
import org.ray.streaming.state.keystate.state.proxy.MapStateStoreManagerProxy;
import org.ray.streaming.state.keystate.state.proxy.ValueStateStoreManagerProxy;

/**
 * key state backend manager, managing different kinds of states.
 */
public class KeyStateBackend extends TransactionKeyStateBackend {

  protected final int numberOfKeyGroups;
  protected final KeyGroup keyGroup;

  public KeyStateBackend(int numberOfKeyGroups, KeyGroup keyGroup,
                         AbstractStateBackend abstractStateBackend) {
    super(abstractStateBackend);
    this.numberOfKeyGroups = numberOfKeyGroups;
    this.keyGroup = keyGroup;
  }

  /**
   * VALUE State
   */
  protected <T> ValueStateStoreManagerProxy<T> newValueStateFacade(
      ValueStateDescriptor<T> stateDescriptor) {
    return new ValueStateStoreManagerProxy<>(this, stateDescriptor);
  }

  public <T> ValueState<T> getValueState(ValueStateDescriptor<T> stateDescriptor) {
    String desc = stateDescriptor.getIdentify();
    if (valueStateMngMap.containsKey(desc)) {
      return valueStateMngMap.get(desc).getValueState();
    } else {
      ValueStateStoreManagerProxy<T> valueStateFacade = newValueStateFacade(stateDescriptor);
      valueStateMngMap.put(desc, valueStateFacade);
      return valueStateFacade.getValueState();
    }
  }

  /**
   * LIST State
   */
  protected <T> ListStateStoreManagerProxy<T> newListStateProxy(
      ListStateDescriptor<T> stateDescriptor) {
    return new ListStateStoreManagerProxy<>(this, stateDescriptor);
  }

  public <T> ListState<T> getListState(ListStateDescriptor<T> stateDescriptor) {
    String desc = stateDescriptor.getIdentify();
    if (listStateMngMap.containsKey(desc)) {
      ListStateStoreManagerProxy<T> listStateFacade = listStateMngMap.get(desc);
      return listStateFacade.getListState();
    } else {
      ListStateStoreManagerProxy<T> listStateProxy = newListStateProxy(stateDescriptor);
      listStateMngMap.put(desc, listStateProxy);
      return listStateProxy.getListState();
    }
  }

  /**
   * MAP state
   */
  protected <S, T> MapStateStoreManagerProxy<S, T> newMapStateFacade(
      MapStateDescriptor<S, T> stateDescriptor) {
    return new MapStateStoreManagerProxy<>(this, stateDescriptor);
  }

  public <S, T> MapState<S, T> getMapState(MapStateDescriptor<S, T> stateDescriptor) {
    String desc = stateDescriptor.getIdentify();
    if (mapStateMngMap.containsKey(desc)) {
      MapStateStoreManagerProxy<S, T> mapStateFacade = mapStateMngMap.get(desc);
      return mapStateFacade.getMapState();
    } else {
      MapStateStoreManagerProxy<S, T> mapStateFacade = newMapStateFacade(stateDescriptor);
      mapStateMngMap.put(desc, mapStateFacade);
      return mapStateFacade.getMapState();
    }
  }

  @Override
  public void setCurrentKey(Object currentKey) {
    super.keyGroupIndex = KeyGroupAssignment.assignKeyGroupIndexForKey(
      currentKey, numberOfKeyGroups);
    super.currentKey = currentKey;
  }

  public int getNumberOfKeyGroups() {
    return numberOfKeyGroups;
  }

  public KeyGroup getKeyGroup() {
    return keyGroup;
  }

  public void close() {
    for (ValueStateStoreManagerProxy facade : valueStateMngMap.values()) {
      facade.close();
    }
    for (ListStateStoreManagerProxy facade : listStateMngMap.values()) {
      facade.close();
    }
    for (MapStateStoreManagerProxy facade : mapStateMngMap.values()) {
      facade.close();
    }
  }
}
