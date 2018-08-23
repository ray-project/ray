package org.ray.api;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import javax.xml.bind.DatatypeConverter;

/**
 * Unique ID for task, worker, function...
 */
public class UniqueID implements Serializable {

  public static final int LENGTH = 20;
  public static final UniqueID NIL = genNilInternal();
  private static final long serialVersionUID = 8588849129675565761L;
  private byte[] id;

  public static UniqueID fromHexString(String hex) {
    byte[] bytes = DatatypeConverter.parseHexBinary(hex);
    return new UniqueID(bytes);
  }

  public static UniqueID fromByteBuffer(ByteBuffer bb) {
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
    if (id.length != LENGTH) {
      throw new IllegalArgumentException("Illegal argument: " + id.toString());
    }

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
    return DatatypeConverter.printHexBinary(id);
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
