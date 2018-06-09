package org.ray.api;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.Random;

/**
 * Unique ID for task, worker, function...
 */
public class UniqueID implements Serializable {

  public static final int LENGTH = 20;
  public static final UniqueID nil = genNil();
  private static final long serialVersionUID = 8588849129675565761L;
  byte[] id;

  public UniqueID(byte[] id) {
    this.id = id;
  }

  public UniqueID(ByteBuffer bb) {
    assert (bb.remaining() == LENGTH);
    id = new byte[bb.remaining()];
    bb.get(id);
  }

  public UniqueID(String optionValue) {
    assert (optionValue.length() == 2 * LENGTH);
    int j = 0;

    id = new byte[LENGTH];
    for (int i = 0; i < LENGTH; i++) {
      char c1 = optionValue.charAt(j++);
      char c2 = optionValue.charAt(j++);
      int first = c1 <= '9' ? (c1 - '0') : (c1 - 'a' + 0xa);
      int second = c2 <= '9' ? (c2 - '0') : (c2 - 'a' + 0xa);
      id[i] = (byte) (first * 16 + second);
    }
  }

  public static UniqueID genNil() {
    byte[] b = new byte[LENGTH];
    for (int i = 0; i < b.length; i++) {
      b[i] = (byte) 0xFF;
    }

    return new UniqueID(b);
  }

  public static UniqueID randomId() {
    byte[] b = new byte[LENGTH];
    new Random().nextBytes(b);
    return new UniqueID(b);
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
    int hash = 0xdeadbeef;
    IntBuffer bb = ByteBuffer.wrap(id).asIntBuffer();
    while (bb.hasRemaining()) {
      hash ^= bb.get();
    }
    return hash;
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
