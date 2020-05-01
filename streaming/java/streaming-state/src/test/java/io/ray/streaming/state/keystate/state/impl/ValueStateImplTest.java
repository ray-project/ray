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

import io.ray.streaming.state.backend.KeyStateBackend;
import io.ray.streaming.state.backend.impl.MemoryStateBackend;
import io.ray.streaming.state.keystate.KeyGroup;
import io.ray.streaming.state.keystate.desc.ValueStateDescriptor;
import java.util.HashMap;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ValueStateImplTest {

  ValueStateImpl<String> valueState;
  KeyStateBackend keyStateBackend;

  @BeforeClass
  public void setUp() throws Exception {
    keyStateBackend = new KeyStateBackend(1, new KeyGroup(1, 2),
        new MemoryStateBackend(new HashMap<>()));
    ValueStateDescriptor<String> descriptor = ValueStateDescriptor
        .build("ValueStateImplTest", String.class, "hello");
    descriptor.setTableName("table");

    valueState = (ValueStateImpl<String>) keyStateBackend.getValueState(descriptor);
  }

  @Test
  public void testUpdateGet() throws Exception {
    keyStateBackend.setContext(1L, 1);

    Assert.assertEquals(valueState.get(), "hello");

    String str = valueState.get();

    valueState.update(str + " world");
    Assert.assertEquals(valueState.get(), "hello world");
  }
}
