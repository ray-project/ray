package io.ray.streaming.runtime.python;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@SuppressWarnings("unchecked")
public class MsgPackSerializerTest {

  @Test
  public void testSerialize() {
    MsgPackSerializer serializer = new MsgPackSerializer();

    Map map = new HashMap();
    List list = new ArrayList<>();
    list.add(null);
    list.add(true);
    list.add(1);
    list.add(1.0d);
    list.add("str");
    map.put("k1", "value1");
    map.put("k2", 2);
    map.put("k3", list);
    byte[] bytes = serializer.serialize(map);
    Object o = serializer.deserialize(bytes);
    assertEquals(o, map);

    byte[] binary = {1, 2, 3, 4};
    assertTrue(Arrays.equals(
        binary, (byte[]) (serializer.deserialize(serializer.serialize(binary)))));
  }

}