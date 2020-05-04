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

package io.ray.streaming.state.backend;

/**
 * This class describe State Saving Model.
 */
public enum StateStrategy {
  /**
   * save two version together in case of rollback.
   */
  DUAL_VERSION,

  /**
   * for storage supporting mvcc, we save only current version.
   */
  SINGLE_VERSION;


  public static StateStrategy getEnum(String value) {
    for (StateStrategy v : values()) {
      if (v.name().equalsIgnoreCase(value)) {
        return v;
      }
    }
    throw new IllegalArgumentException(value + " strategy is not supported");
  }
}
