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

import io.ray.streaming.state.keystate.desc.ValueStateDescriptor;
import io.ray.streaming.state.keystate.state.ValueState;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ValueStateStoreManagerTest extends StateStoreManagerTest {

  ValueStateStoreManagerProxy<Integer> proxy;

  @BeforeClass
  public void setUp() {
    ValueStateDescriptor<Integer> descriptor = ValueStateDescriptor
        .build("value", Integer.class, 0);
    descriptor.setTableName("tableName");
    keyStateBackend.setContext(1L, "key");
    proxy = new ValueStateStoreManagerProxy<>(keyStateBackend, descriptor);
  }

  @Test
  public void test() throws Exception {
    ValueState<Integer> state = proxy.getValueState();
    Assert.assertEquals(state.get().intValue(), 0);

    proxy.put("key1", 2);
    Assert.assertEquals(proxy.get("key1").intValue(), 2);

    proxy.put("key1", 3);
    Assert.assertEquals(proxy.get("key1").intValue(), 3);

    proxy.put("key2", 9);
    Assert.assertEquals(proxy.get("key2").intValue(), 9);

    proxy.put("key2", 6);
    Assert.assertEquals(proxy.get("key2").intValue(), 6);
  }
}
