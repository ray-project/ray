package org.ray.streaming.python;

import static org.testng.Assert.*;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

@SuppressWarnings("unchecked")
public class MsgPackSerializerTest {

  @Test
  public void testSerialize() {
    MsgPackSerializer serializer = new MsgPackSerializer();
    Map map = new HashMap();
    List list = new ArrayList<>();
    list.add(null);
    list.add(1);
    list.add("str");
    map.put("k1", "value1");
    map.put("k2", 2);
    map.put("k3", list);
    byte[] bytes = serializer.serialize(map);
    Object o = serializer.deserialize(bytes);
    assertEquals(o, map);
  }
}