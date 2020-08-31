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

import com.google.common.base.Preconditions;
import io.ray.streaming.state.keystate.state.State;

/**
 * This class defines basic data structures of StateDescriptor.
 */
public abstract class AbstractStateDescriptor<S extends State, T> {

  private final String name;
  private String tableName;
  private Class<T> type;

  protected AbstractStateDescriptor(String name, Class<T> type) {
    this.name = name;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public Class<T> getType() {
    return type;
  }

  protected Class<T> setType(Class<T> type) {
    return type;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public abstract StateType getStateType();

  public String getIdentify() {
    Preconditions.checkArgument(this.tableName != null, "table name must not be null.");
    Preconditions.checkArgument(this.name != null, "table name must not be null.");
    return this.name;
  }

  @Override
  public String toString() {
    return "AbstractStateDescriptor{" + "tableName='" + tableName + '\'' + ", name='" + name + '\''
        + ", type=" + type + '}';
  }

  public enum StateType {
    /**
     * value state
     */
    VALUE,

    /**
     * list state
     */
    LIST,

    /**
     * map state
     */
    MAP
  }
}
