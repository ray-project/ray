package io.ray.serialization.serializers;

import static org.testng.Assert.assertEquals;

import com.google.common.base.Preconditions;
import io.ray.serialization.Fury;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import lombok.Data;
import org.testng.annotations.Test;

public class ExternalizableSerializerTest {

  @Data
  public static class A implements Externalizable {
    private int x;
    private int y;
    private byte[] bytes;

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      out.writeInt(x);
      out.writeInt(y);
      out.writeInt(bytes.length);
      out.write(bytes);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      this.x = in.readInt();
      this.y = in.readInt();
      int len = in.readInt();
      byte[] arr = new byte[len];
      Preconditions.checkArgument(in.read(arr) == len);
      this.bytes = arr;
    }
  }

  @Test
  public void testExternalizable() {
    A a = new A();
    a.x = 1;
    a.y = 1;
    a.bytes = "bytes".getBytes();

    Fury fury = Fury.builder().withReferenceTracking(false).build();
    assertEquals(a, fury.deserialize(fury.serialize(a)));
  }
}
