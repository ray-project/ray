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

package io.ray.streaming.state.keystate.desc;

import static io.ray.streaming.state.config.ConfigKey.DELIMITER;

import io.ray.streaming.state.keystate.state.ListState;

/** ListStateDescriptor. */
public class ListStateDescriptor<T> extends AbstractStateDescriptor<ListState<T>, T> {

  private final boolean isOperatorList;
  private int index;
  private int partitionNum;

  private ListStateDescriptor(String name, Class<T> type, boolean isOperatorList) {
    super(name, type);
    this.isOperatorList = isOperatorList;
  }

  public static <T> ListStateDescriptor<T> build(String name, Class<T> type) {
    return build(name, type, false);
  }

  public static <T> ListStateDescriptor<T> build(
      String name, Class<T> type, boolean isOperatorList) {
    return new ListStateDescriptor<>(name, type, isOperatorList);
  }

  public boolean isOperatorList() {
    return isOperatorList;
  }

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  public int getPartitionNumber() {
    return partitionNum;
  }

  public void setPartitionNumber(int number) {
    this.partitionNum = number;
  }

  @Override
  public StateType getStateType() {
    return StateType.LIST;
  }

  @Override
  public String getIdentify() {
    if (isOperatorList) {
      return String.format(
          "%s%s%d%s%d", super.getIdentify(), DELIMITER, partitionNum, DELIMITER, index);
    } else {
      return super.getIdentify();
    }
  }
}
