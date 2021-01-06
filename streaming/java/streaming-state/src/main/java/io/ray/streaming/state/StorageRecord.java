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

/** This Class contains a record with some checkpointId. */
public class StorageRecord<T> implements Serializable {

  private long checkpointId;
  private T value;

  public StorageRecord() {}

  public StorageRecord(long checkpointId, T value) {
    this.checkpointId = checkpointId;
    this.value = value;
  }

  public T getValue() {
    return value;
  }

  public long getCheckpointId() {
    return checkpointId;
  }

  public void setCheckpointId(long checkpointId) {
    this.checkpointId = checkpointId;
  }

  @Override
  public String toString() {
    if (value != null) {
      return "checkpointId:" + checkpointId + ", value:" + value;
    } else {
      return "checkpointId:" + checkpointId + ", value:null";
    }
  }
}
