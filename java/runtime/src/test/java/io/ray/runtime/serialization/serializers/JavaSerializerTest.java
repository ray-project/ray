package io.ray.runtime.serialization.serializers;

import io.ray.runtime.serialization.RaySerde;
import java.io.Serializable;
import org.testng.annotations.Test;

public class JavaSerializerTest {

  public static class SerializedForm implements Serializable {
    private final Object[] fields;

    public SerializedForm(Object... fields) {
      this.fields = fields;
    }
  }

  public static class A implements Serializable {
    int f1 = 1;
    Object writeReplace() {
      return new SerializedForm("A", f1);
    }
  }

  public static class B {
    int f2 = 2;
  }

  @Test
  public void testReplace() {
    RaySerde serde = RaySerde.builder().build();
    byte[] bytes = serde.serialize(new A());
    serde.deserialize(bytes);
  }
}
