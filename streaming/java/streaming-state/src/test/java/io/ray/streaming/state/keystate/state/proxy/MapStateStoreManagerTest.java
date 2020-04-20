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

import io.ray.streaming.state.keystate.desc.MapStateDescriptor;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class MapStateStoreManagerTest extends StateStoreManagerTest {

  MapStateStoreManagerProxy<String, Integer> facade;

  @BeforeClass
  public void setUp() {
    MapStateDescriptor<String, Integer> descriptor = MapStateDescriptor
        .build("map", String.class, Integer.class);
    descriptor.setTableName("tableName");
    keyStateBackend.setContext(1L, "key");
    facade = new MapStateStoreManagerProxy<>(keyStateBackend, descriptor);
  }

  @Test
  public void test() throws Exception {
    Assert.assertEquals(facade.getMapState().get().size(), 0);

    Map<String, Integer> map = new HashMap<>();
    map.put("key1", 1);
    map.put("key2", 2);
    facade.put("key1", map);
    Assert.assertEquals(facade.get("key1"), map);

    map.remove("key1");
    facade.put("key2", map);
    Assert.assertEquals(facade.get("key2"), map);
  }

}
