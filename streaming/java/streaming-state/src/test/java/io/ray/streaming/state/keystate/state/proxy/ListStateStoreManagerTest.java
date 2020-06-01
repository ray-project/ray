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

import io.ray.streaming.state.keystate.desc.ListStateDescriptor;
import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ListStateStoreManagerTest extends StateStoreManagerTest {

  ListStateStoreManagerProxy<Integer> proxy;

  @BeforeClass
  public void setUp() {
    ListStateDescriptor<Integer> descriptor = ListStateDescriptor.build("list", Integer.class);
    descriptor.setTableName("tableName");
    keyStateBackend.setContext(1L, "key");
    proxy = new ListStateStoreManagerProxy<>(keyStateBackend, descriptor);
  }

  @Test
  public void test() throws Exception {
    Assert.assertEquals(proxy.getListState().get().size(), 0);

    List<Integer> list = Arrays.asList(1, 2, 3);
    proxy.put("key1", list);
    Assert.assertEquals(proxy.get("key1"), list);

    proxy.put("key1", Arrays.asList(1, 3));

    proxy.put("key2", Arrays.asList(4, 5));
    Assert.assertEquals(proxy.get("key2"), Arrays.asList(4, 5));

    Assert.assertEquals(proxy.get("key1"), Arrays.asList(1, 3));
  }
}
