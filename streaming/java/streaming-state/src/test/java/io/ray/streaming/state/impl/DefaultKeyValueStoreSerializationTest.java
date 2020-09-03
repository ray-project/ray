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

import io.ray.streaming.state.serialization.impl.DefaultKeyValueStoreSerialization;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DefaultKeyValueStoreSerializationTest {

  DefaultKeyValueStoreSerialization<String, Integer> serDe =
      new DefaultKeyValueStoreSerialization<>();
  byte[] ret;

  @Test
  public void testSerKey() throws Exception {
    ret = serDe.serializeKey("key");
    String key = new String(ret);
    Assert.assertEquals(key.indexOf("key"), 5);
  }

  @Test
  public void testSerValue() throws Exception {
    ret = serDe.serializeValue(5);
    Assert.assertEquals(ret.length, 2);
    Assert.assertEquals((int) serDe.deserializeValue(ret), 5);
  }
}
