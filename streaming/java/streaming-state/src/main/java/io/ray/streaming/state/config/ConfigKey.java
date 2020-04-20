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

package io.ray.streaming.state.config;

import java.util.Map;

/**
 * state config keys.
 * Todo replace this to config module.
 */
public final class ConfigKey {

  /**
   * backend
   */
  public static final String STATE_BACKEND_TYPE = "state.backend.type";
  public static final String STATE_TABLE_NAME = "state.table.name";
  public static final String STATE_STRATEGY_MODE = "state.strategy.mode";
  public static final String NUMBER_PER_CHECKPOINT = "number.per.checkpoint";
  public static final String JOB_MAX_PARALLEL = "job.max.parallel";
  public static final String DELIMITER = "\u0001\u0008"; // for String delimiter

  private ConfigKey() {
    throw new AssertionError();
  }

  public static String getStateStrategyEnum(Map<String, String> config) {
    return ConfigHelper.getStringOrDefault(config, STATE_STRATEGY_MODE, "DUAL_VERSION");
  }

  public static String getBackendType(Map<String, String> config) {
    return ConfigHelper.getStringOrDefault(config, STATE_BACKEND_TYPE, "MEMORY");
  }

  public static int getNumberPerCheckpoint(Map<String, String> config) {
    return ConfigHelper.getIntegerOrDefault(config, NUMBER_PER_CHECKPOINT, 5);
  }

  public static String getStateTableName(Map<String, String> config) {
    return ConfigHelper.getStringOrDefault(config, STATE_TABLE_NAME, "table");
  }
}
