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

import io.ray.streaming.state.backend.OperatorStateBackend;
import io.ray.streaming.state.backend.impl.MemoryStateBackend;
import io.ray.streaming.state.keystate.desc.ListStateDescriptor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class OperatorStateImplTest {

  OperatorStateImpl<Integer> operatorState;
  ListStateDescriptor<Integer> descriptor;
  OperatorStateBackend operatorStateBackend;

  @BeforeClass
  public void setUp() {
    String table_name = "operatorState";
    Map config = new HashMap<>();

    operatorStateBackend = new OperatorStateBackend(new MemoryStateBackend(config));

    descriptor =
        ListStateDescriptor.build(
            "OperatorStateImplTest" + System.currentTimeMillis(), Integer.class, true);
    descriptor.setPartitionNumber(1);
    descriptor.setIndex(0);
    descriptor.setTableName(table_name);

    operatorState = (OperatorStateImpl<Integer>) operatorStateBackend.getSplitListState(descriptor);
  }

  @Test
  public void testInit() throws Exception {
    operatorStateBackend.setCheckpointId(1L);
    List<Integer> list = operatorState.get();
    Assert.assertEquals(list.size(), 0);

    for (int i = 0; i < 100; i++) {
      operatorState.add(i);
    }
    Assert.assertEquals(operatorState.get().size(), 100);
    operatorStateBackend.finish(1L);
    operatorStateBackend.commit(1L);
    operatorStateBackend.ackCommit(1L, 0);

    operatorStateBackend.finish(5L);
    operatorStateBackend.commit(5L);
    operatorStateBackend.ackCommit(5L, 0);
  }
}
