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

package org.ray.streaming.state.impl;

import java.util.HashMap;
import java.util.Map;
import org.ray.streaming.state.serde.impl.DefaultKMapStoreSerDe;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DefaultKMapStoreSerDeTest {

  private DefaultKMapStoreSerDe<String, String, Map<String, String>> defaultKMapStoreSerDe;

  @BeforeClass
  public void setUp() {
    this.defaultKMapStoreSerDe = new DefaultKMapStoreSerDe<>();
  }

  @Test
  public void testSerKey() {
    String key = "hello";
    byte[] result = this.defaultKMapStoreSerDe.serKey(key);
    String keyWithPrefix = this.defaultKMapStoreSerDe.generateRowKeyPrefix(key.toString());
    Assert.assertEquals(result, keyWithPrefix.getBytes());
  }

  @Test
  public void testSerUKey() {
    String subKey = "hell1";
    byte[] result = this.defaultKMapStoreSerDe.serUKey(subKey);
    Assert.assertEquals(subKey, this.defaultKMapStoreSerDe.deSerUKey(result));
  }

  @Test
  public void testSerUValue() {
    Map<String, String> value = new HashMap<>();
    value.put("foo", "bar");
    byte[] result = this.defaultKMapStoreSerDe.serUValue(value);
    Assert.assertEquals(value, this.defaultKMapStoreSerDe.deSerUValue(result));
  }

}