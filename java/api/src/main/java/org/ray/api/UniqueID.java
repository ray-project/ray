package org.ray.api;

import javax.xml.bind.DatatypeConverter;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.Random;

/**
 * Unique ID for task, worker, function...
 */
public class UniqueID implements Serializable {

  public static final int LENGTH = 20;
  public static final UniqueID NIL = genNilInternal();
  private static final long serialVersionUID = 8588849129675565761L;
  byte[] id;

  public static UniqueID fromHex(String hex) {
    if (hex.length() != 2 * LENGTH) {
      throw new IllegalArgumentException("The argument is illegal.");
    }

    byte[] bytes = DatatypeConverter.parseHexBinary(hex);
    return new UniqueID(bytes);
  }

  public static UniqueID fromByteBuffer(ByteBuffer bb) {
    if (bb.remaining() != LENGTH) {
      throw new IllegalArgumentException("The argument is illegal.");
    }

    byte[] id = new byte[bb.remaining()];
    bb.get(id);

    return new UniqueID(id);
  }

  public static UniqueID genNil() {
    return NIL.copy();
  }

  public static UniqueID randomId() {
    byte[] b = new byte[LENGTH];
    new Random().nextBytes(b);
    return new UniqueID(b);
  }

  private static UniqueID genNilInternal() {
    byte[] b = new byte[LENGTH];
    Arrays.fill(b, (byte) 0xFF);
    return new UniqueID(b);
  }

  public UniqueID(byte[] id) {
    this.id = id;
  }

  public byte[] getBytes() {
    return id;
  }

  public ByteBuffer toByteBuffer() {
    return ByteBuffer.wrap(id);
  }

  public UniqueID copy() {
    byte[] nid = Arrays.copyOf(id, id.length);
    return new UniqueID(nid);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(id);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }

    if (!(obj instanceof UniqueID)) {
      return false;
    }

    UniqueID r = (UniqueID) obj;
    return Arrays.equals(id, r.id);
  }

  @Override
  public String toString() {
    String s = "";
    String hex = "0123456789abcdef";
    for (int i = 0; i < LENGTH; i++) {
      int val = id[i] & 0xff;
      s += hex.charAt(val >> 4);
      s += hex.charAt(val & 0xf);
    }
    return s;
  }

  public boolean isNil() {
    for (byte b : id) {
      if (b != (byte) 0xFF) {
        return false;
      }
    }
    return true;
  }
}
