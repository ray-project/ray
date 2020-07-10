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

package io.ray.streaming.state;

import java.io.Serializable;

/**
 * value record for partition.
 */
public class PartitionRecord<T> implements Serializable {

  /**
   * The partition number of the partitioned value.
   */
  private int partitionID;
  private T value;

  public PartitionRecord() {
  }

  public PartitionRecord(int partitionID, T value) {
    this.partitionID = partitionID;
    this.value = value;
  }

  public T getValue() {
    return value;
  }

  public int getPartitionID() {
    return partitionID;
  }

  public void setPartitionID(int partitionID) {
    this.partitionID = partitionID;
  }
}
