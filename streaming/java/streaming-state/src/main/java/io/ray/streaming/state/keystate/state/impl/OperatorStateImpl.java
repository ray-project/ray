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

import static io.ray.streaming.state.config.ConfigKey.DELIMITER;

import com.google.common.base.Preconditions;
import io.ray.streaming.state.PartitionRecord;
import io.ray.streaming.state.backend.AbstractKeyStateBackend;
import io.ray.streaming.state.keystate.desc.ListStateDescriptor;
import io.ray.streaming.state.keystate.state.ListState;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class defines the implementation of operator state.
 * When the state is initialized, we must scan the whole table.
 * And if the state type is splitList, all the records must be spitted.
 */
public class OperatorStateImpl<V> implements ListState<V> {

  private final ListStateDescriptor<V> descriptor;
  private StateHelper<List<PartitionRecord<V>>> helper;
  private List<PartitionRecord<V>> allList;

  private AtomicBoolean hasInit;
  private boolean isSplit;

  public OperatorStateImpl(ListStateDescriptor<V> descriptor, AbstractKeyStateBackend backend) {
    this.descriptor = descriptor;
    this.helper = new StateHelper<>(backend, descriptor);
    this.isSplit = false;
    this.hasInit = new AtomicBoolean(false);
    this.allList = new ArrayList<>();
  }

  private void splitList() {
    // fetch target list and save
    List<PartitionRecord<V>> list = new ArrayList<>();
    int step = descriptor.getPartitionNumber();
    Preconditions.checkArgument(step > 0);

    for (int round = 0; round * step <= allList.size(); round++) {
      int m = round * step + descriptor.getIndex();
      if (m < allList.size()) {
        PartitionRecord<V> tmp = allList.get(m);
        tmp.setPartitionID(descriptor.getPartitionNumber());
        list.add(tmp);
      }
    }
    helper.put(list, getStateKey());
    allList.clear();
  }

  private void scan() {
    int partitionNum = -1;
    int index = 0;
    while (true) {
      List<PartitionRecord<V>> list = helper.getBackend()
          .get(descriptor, getKey(descriptor.getIdentify(), index));
      if (list != null && !list.isEmpty()) {
        partitionNum = list.get(0).getPartitionID();
        allList.addAll(list);
      }
      if (++index >= partitionNum) {
        break;
      }
    }
  }

  public void init() {
    scan();

    if (isSplit) {
      splitList();
    }
  }

  private String getKey(String descName, int index) {
    String[] stringList = descName.split(DELIMITER);
    return String.format("%s%s%s%s%d", stringList[0], DELIMITER, stringList[1], DELIMITER, index);
  }

  protected String getStateKey() {
    return getKey(this.descriptor.getIdentify(), this.descriptor.getIndex());
  }

  @Override
  public void setCurrentKey(Object currentKey) {
    throw new UnsupportedOperationException("OperatorState cannot set current key");
  }

  @Override
  public List<V> get() {
    if (!hasInit.getAndSet(true)) {
      init();
    }
    List<PartitionRecord<V>> prList = helper.get(getStateKey());
    List<V> list = new ArrayList<>();
    for (PartitionRecord<V> pr : prList) {
      list.add(pr.getValue());
    }
    return list;
  }

  @Override
  public void add(V value) {
    if (!hasInit.getAndSet(true)) {
      init();
    }
    List<PartitionRecord<V>> list = helper.get(getStateKey());
    if (list == null) {
      list = new ArrayList<>();
    }
    list.add(new PartitionRecord<>(descriptor.getPartitionNumber(), value));
    helper.put(list, getStateKey());
  }

  @Override
  public void update(List<V> list) {
    List<PartitionRecord<V>> prList = new ArrayList<>();
    if (list != null) {
      for (V value : list) {
        prList.add(new PartitionRecord<>(descriptor.getPartitionNumber(), value));
      }
    }
    helper.put(prList);
  }

  public void setSplit(boolean split) {
    this.isSplit = split;
  }
}
