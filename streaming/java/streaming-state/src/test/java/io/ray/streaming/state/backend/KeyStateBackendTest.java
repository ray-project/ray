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

import io.ray.streaming.state.keystate.KeyGroup;
import io.ray.streaming.state.keystate.desc.ListStateDescriptor;
import io.ray.streaming.state.keystate.desc.MapStateDescriptor;
import io.ray.streaming.state.keystate.desc.ValueStateDescriptor;
import io.ray.streaming.state.keystate.state.ListState;
import io.ray.streaming.state.keystate.state.MapState;
import io.ray.streaming.state.keystate.state.ValueState;
import java.util.Arrays;
import java.util.HashMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class KeyStateBackendTest {

  private AbstractStateBackend stateBackend;
  private KeyStateBackend keyStateBackend;

  public void testGetValueState() {
    keyStateBackend.setCheckpointId(1L);
    ValueStateDescriptor<String> valueStateDescriptor = ValueStateDescriptor
      .build("value", String.class, null);
    valueStateDescriptor.setTableName("kepler_hlg_ut");
    ValueState<String> valueState = keyStateBackend.getValueState(valueStateDescriptor);

    valueState.setCurrentKey("1");
    valueState.update("hello");
    Assert.assertEquals(valueState.get(), "hello");

    valueState.update("hello1");
    Assert.assertEquals(valueState.get(), "hello1");

    valueState.setCurrentKey("2");
    Assert.assertEquals(valueState.get(), null);

    valueState.update("eagle");
    Assert.assertEquals(valueState.get(), "eagle");

    keyStateBackend.rollBack(1);
    valueState.setCurrentKey("1");
    Assert.assertEquals(valueState.get(), null);
    valueState.setCurrentKey("2");
    Assert.assertEquals(valueState.get(), null);

    valueState.setCurrentKey("1");
    valueState.update("eagle");
    keyStateBackend.finish(1);

    keyStateBackend.setCheckpointId(2);
    valueState.setCurrentKey("2");
    valueState.update("tim");

    valueState.setCurrentKey("2-1");
    valueState.update("jim");
    keyStateBackend.finish(2);

    keyStateBackend.setCheckpointId(3);
    valueState.setCurrentKey("3");
    valueState.update("lucy");
    keyStateBackend.finish(3);

    keyStateBackend.setCheckpointId(4);
    valueState.setCurrentKey("4");
    valueState.update("eric");
    keyStateBackend.finish(4);

    keyStateBackend.setCheckpointId(5);
    valueState.setCurrentKey("4");
    valueState.update("eric-1");
    valueState.setCurrentKey("5");
    valueState.update("jack");
    keyStateBackend.finish(5);
    keyStateBackend.commit(5);

    keyStateBackend.setCheckpointId(6);
    valueState.setCurrentKey("5");
    Assert.assertEquals(valueState.get(), "jack");

    valueState.setCurrentKey("4");
    Assert.assertEquals(valueState.get(), "eric-1");

    valueState.setCurrentKey(4);
    valueState.update("if-ttt");
    Assert.assertEquals(valueState.get(), "if-ttt");

    keyStateBackend.setCheckpointId(7);
    valueState.setCurrentKey(9);
    valueState.update("6666");

    keyStateBackend.rollBack(5);
    keyStateBackend.setCheckpointId(6);
    valueState.setCurrentKey("4");
    Assert.assertEquals(valueState.get(), "eric-1");
    valueState.setCurrentKey("5");
    Assert.assertEquals(valueState.get(), "jack");
    valueState.setCurrentKey("9");
    Assert.assertNull(valueState.get());

  }

  public void testGetListState() {
    keyStateBackend.setCheckpointId(1l);
    ListStateDescriptor<String> listStateDescriptor = ListStateDescriptor
      .build("list", String.class);
    listStateDescriptor.setTableName("kepler_hlg_ut");
    ListState<String> listState = keyStateBackend.getListState(listStateDescriptor);

    listState.setCurrentKey("1");
    listState.add("hello1");
    Assert.assertEquals(listState.get(), Arrays.asList("hello1"));

    listState.add("hello2");
    Assert.assertEquals(listState.get(), Arrays.asList("hello1", "hello2"));

    listState.setCurrentKey("2");
    Assert.assertEquals(listState.get(), Arrays.asList());

    listState.setCurrentKey("2");
    listState.add("eagle");
    listState.setCurrentKey("1");
    Assert.assertEquals(listState.get(), Arrays.asList("hello1", "hello2"));
    listState.setCurrentKey("2");
    Assert.assertEquals(listState.get(), Arrays.asList("eagle"));

    keyStateBackend.rollBack(1);
    listState.setCurrentKey("1");
    Assert.assertEquals(listState.get(), Arrays.asList());
    listState.setCurrentKey("2");
    Assert.assertEquals(listState.get(), Arrays.asList());

    listState.setCurrentKey("1");
    listState.add("eagle");
    listState.add("eagle-2");
    keyStateBackend.finish(1);

    keyStateBackend.setCheckpointId(2);
    listState.setCurrentKey("2");
    listState.add("tim");

    listState.setCurrentKey("2-1");
    listState.add("jim");
    keyStateBackend.finish(2);

    keyStateBackend.setCheckpointId(3);
    listState.setCurrentKey("3");
    listState.add("lucy");
    keyStateBackend.finish(3);

    keyStateBackend.setCheckpointId(4);
    listState.setCurrentKey("4");
    listState.add("eric");
    keyStateBackend.finish(4);

    keyStateBackend.setCheckpointId(5);
    listState.setCurrentKey("4");
    listState.add("eric-1");
    Assert.assertEquals(listState.get(), Arrays.asList("eric", "eric-1"));

    listState.setCurrentKey("5");
    listState.add("jack");
    keyStateBackend.finish(5);
    keyStateBackend.commit(5);

    keyStateBackend.setCheckpointId(6);
    listState.setCurrentKey("5");
    Assert.assertEquals(listState.get(), Arrays.asList("jack"));

    listState.setCurrentKey("4");
    Assert.assertEquals(listState.get(), Arrays.asList("eric", "eric-1"));

    listState.setCurrentKey(4);
    listState.add("if-ttt");
    Assert.assertEquals(listState.get(), Arrays.asList("eric", "eric-1", "if-ttt"));

    keyStateBackend.setCheckpointId(7);
    listState.setCurrentKey(9);
    listState.add("6666");

    keyStateBackend.rollBack(5);
    keyStateBackend.setCheckpointId(6);
    listState.setCurrentKey("4");
    Assert.assertEquals(listState.get(), Arrays.asList("eric", "eric-1"));
    listState.setCurrentKey("5");
    Assert.assertEquals(listState.get(), Arrays.asList("jack"));
    listState.setCurrentKey("9");
    Assert.assertEquals(listState.get(), Arrays.asList());
  }

  public void testGetMapState() {
    keyStateBackend.setCheckpointId(1l);
    MapStateDescriptor<String, String> mapStateDescriptor = MapStateDescriptor
      .build("map", String.class, String.class);
    mapStateDescriptor.setTableName("kepler_hlg_ut");
    MapState<String, String> mapState = keyStateBackend.getMapState(mapStateDescriptor);

    mapState.setCurrentKey("1");
    mapState.put("hello1", "world1");
    Assert.assertEquals(mapState.get("hello1"), "world1");

    mapState.put("hello2", "world2");
    Assert.assertEquals(mapState.get("hello2"), "world2");
    Assert.assertEquals(mapState.get("hello1"), "world1");
    Assert.assertEquals(mapState.get("hello3"), null);

    mapState.setCurrentKey("2");
    //Assert.assertEquals(mapState.iterator(), (new HashMap()));

    mapState.setCurrentKey("2");
    mapState.put("eagle", "eagle-1");
    mapState.setCurrentKey("1");
    Assert.assertEquals(mapState.get("hello1"), "world1");
    mapState.setCurrentKey("2");
    Assert.assertEquals(mapState.get("eagle"), "eagle-1");
    Assert.assertEquals(mapState.get("xxx"), null);

    keyStateBackend.rollBack(1);
    mapState.setCurrentKey("1");
    Assert.assertEquals(mapState.iterator(), (new HashMap()).entrySet().iterator());
    mapState.setCurrentKey("2");
    Assert.assertEquals(mapState.iterator(), (new HashMap()).entrySet().iterator());

    mapState.setCurrentKey("1");
    mapState.put("eagle", "eagle-1");
    mapState.put("eagle-2", "eagle-3");
    keyStateBackend.finish(1);

    keyStateBackend.setCheckpointId(2);
    mapState.setCurrentKey("2");
    mapState.put("tim", "tina");

    mapState.setCurrentKey("2-1");
    mapState.put("jim", "tick");
    keyStateBackend.finish(2);

    keyStateBackend.setCheckpointId(3);
    mapState.setCurrentKey("3");
    mapState.put("lucy", "ja");
    keyStateBackend.finish(3);

    keyStateBackend.setCheckpointId(4);
    mapState.setCurrentKey("4");
    mapState.put("eric", "sam");
    keyStateBackend.finish(4);

    keyStateBackend.setCheckpointId(5);
    mapState.setCurrentKey("4");
    mapState.put("eric-1", "zxy");
    Assert.assertEquals(mapState.get("eric-1"), "zxy");
    Assert.assertEquals(mapState.get("eric"), "sam");

    mapState.setCurrentKey("5");
    mapState.put("jack", "zhang");
    keyStateBackend.finish(5);
    keyStateBackend.commit(5);

    keyStateBackend.setCheckpointId(6);
    mapState.setCurrentKey("5");
    Assert.assertEquals(mapState.get("jack"), "zhang");
    mapState.put("hlll", "gggg");

    mapState.setCurrentKey("4");
    Assert.assertEquals(mapState.get("eric-1"), "zxy");
    Assert.assertEquals(mapState.get("eric"), "sam");

    mapState.setCurrentKey(4);
    mapState.put("if-ttt", "if-ggg");
    Assert.assertEquals(mapState.get("if-ttt"), "if-ggg");

    keyStateBackend.setCheckpointId(7);
    mapState.setCurrentKey(9);
    mapState.put("6666", "7777");

    keyStateBackend.rollBack(5);
    keyStateBackend.setCheckpointId(6);
    mapState.setCurrentKey("4");
    Assert.assertEquals(mapState.get("eric-1"), "zxy");
    Assert.assertEquals(mapState.get("eric"), "sam");
    Assert.assertNull(mapState.get("if-ttt"));

    mapState.setCurrentKey("5");
    Assert.assertNull(mapState.get("hlll"));
    mapState.setCurrentKey("9");
    Assert.assertNull(mapState.get("6666"));
  }


  @Test
  public void testMem() {
    stateBackend = StateBackendBuilder.buildStateBackend(new HashMap<>());
    keyStateBackend = new KeyStateBackend(4, new KeyGroup(2, 3), stateBackend);
    testGetValueState();
    testGetListState();
    testGetMapState();
  }

}
