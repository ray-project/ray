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

package io.ray.streaming.state.impl;

import io.ray.streaming.state.backend.AbstractStateBackend;
import io.ray.streaming.state.backend.StateBackendBuilder;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class MemoryKeyValueStoreTest {

  private AbstractStateBackend stateBackend;
  private io.ray.streaming.state.store.KeyValueStore<String, String> KeyValueStore;

  @BeforeClass
  public void setUp() {
    Map<String, String> config = new HashMap<>();
    stateBackend = StateBackendBuilder.buildStateBackend(config);
    KeyValueStore = stateBackend.getKeyValueStore("kepler_hlg_ut");
  }

  @Test
  public void testCase() {
    try {
      KeyValueStore.put("hello", "world");
      Assert.assertEquals(KeyValueStore.get("hello"), "world");
      KeyValueStore.put("hello", "world1");
      Assert.assertEquals(KeyValueStore.get("hello"), "world1");
      Assert.assertNull(KeyValueStore.get("hello1"));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
