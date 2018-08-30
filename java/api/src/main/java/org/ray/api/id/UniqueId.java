package org.ray.api.id;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import javax.xml.bind.DatatypeConverter;

/**
 * Represents a unique id of all Ray concepts, including
 * objects, tasks, workers, actors, etc.
 */
public class UniqueId implements Serializable {

  public static final int LENGTH = 20;
  public static final UniqueId NIL = genNil();
  private static final long serialVersionUID = 8588849129675565761L;
  private final byte[] id;

  /**
   * Create a UniqueId from a hex string.
   */
  public static UniqueId fromHexString(String hex) {
    byte[] bytes = DatatypeConverter.parseHexBinary(hex);
    return new UniqueId(bytes);
  }

  /**
   * Creates a UniqueId from a ByteBuffer.
   */
  public static UniqueId fromByteBuffer(ByteBuffer bb) {
    byte[] id = new byte[bb.remaining()];
    bb.get(id);

    return new UniqueId(id);
  }

  /**
   * Generate a nil UniqueId.
   */
  public static UniqueId genNil() {
    byte[] b = new byte[LENGTH];
    Arrays.fill(b, (byte) 0xFF);
    return new UniqueId(b);
  }

  /**
   * Generate an UniqueId with random value.
   */
  public static UniqueId randomId() {
    byte[] b = new byte[LENGTH];
    new Random().nextBytes(b);
    return new UniqueId(b);
  }

  public UniqueId(byte[] id) {
    if (id.length != LENGTH) {
      throw new IllegalArgumentException("Illegal argument for UniqueId, expect " + LENGTH
          + " bytes, but got " + id.length + " bytes.");
    }

    this.id = id;
  }

  /**
   * Get the byte data of this UniqueId.
   */
  public byte[] getBytes() {
    return id;
  }

  /**
   * Convert the byte data to a ByteBuffer.
   */
  public ByteBuffer toByteBuffer() {
    return ByteBuffer.wrap(id);
  }

  /**
   * Create a copy of this UniqueId.
   */
  public UniqueId copy() {
    byte[] nid = Arrays.copyOf(id, id.length);
    return new UniqueId(nid);
  }

  /**
   * Returns true if this id is nil.
   */
  public boolean isNil() {
    return this.equals(NIL);
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

    if (!(obj instanceof UniqueId)) {
      return false;
    }

    UniqueId r = (UniqueId) obj;
    return Arrays.equals(id, r.id);
  }

  @Override
  public String toString() {
    return DatatypeConverter.printHexBinary(id);
  }
}
