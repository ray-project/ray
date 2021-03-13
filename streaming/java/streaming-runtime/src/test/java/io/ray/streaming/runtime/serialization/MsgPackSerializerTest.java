package io.ray.streaming.runtime.serialization;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

@SuppressWarnings("unchecked")
public class MsgPackSerializerTest {

  @Test
  public void testSerializeByte() {
    MsgPackSerializer serializer = new MsgPackSerializer();

    assertEquals(serializer.deserialize(serializer.serialize((byte) 1)), (byte) 1);
  }

  @Test
  public void testSerialize() {
    MsgPackSerializer serializer = new MsgPackSerializer();

    assertEquals(serializer.deserialize(serializer.serialize(Short.MAX_VALUE)), Short.MAX_VALUE);
    assertEquals(
        serializer.deserialize(serializer.serialize(Integer.MAX_VALUE)), Integer.MAX_VALUE);
    assertEquals(serializer.deserialize(serializer.serialize(Long.MAX_VALUE)), Long.MAX_VALUE);

    Map map = new HashMap();
    List list = new ArrayList<>();
    list.add(null);
    list.add(true);
    list.add(1.0d);
    list.add("str");
    map.put("k1", "value1");
    map.put("k2", new HashMap<>());
    map.put("k3", list);
    byte[] bytes = serializer.serialize(map);
    Object o = serializer.deserialize(bytes);
    assertEquals(o, map);

    byte[] binary = {1, 2, 3, 4};
    assertTrue(
        Arrays.equals(binary, (byte[]) (serializer.deserialize(serializer.serialize(binary)))));
  }
}
