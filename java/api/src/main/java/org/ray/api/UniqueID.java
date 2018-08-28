package org.ray.api;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import javax.xml.bind.DatatypeConverter;

/**
 * Represents a unique id of all Ray concepts, including
 * objects, tasks, workers, actors, etc.
 */
public class UniqueID implements Serializable {

  public static final int LENGTH = 20;
  public static final UniqueID NIL = genNil();
  private static final long serialVersionUID = 8588849129675565761L;
  private final byte[] id;

  /**
   * Create a UniqueID from a hex string.
   */
  public static UniqueID fromHexString(String hex) {
    byte[] bytes = DatatypeConverter.parseHexBinary(hex);
    return new UniqueID(bytes);
  }

  /**
   * Creates a UniqueID from a ByteBuffer.
   */
  public static UniqueID fromByteBuffer(ByteBuffer bb) {
    byte[] id = new byte[bb.remaining()];
    bb.get(id);

    return new UniqueID(id);
  }

  /**
   * Generate a nil UniqueID.
   */
  public static UniqueID genNil() {
    byte[] b = new byte[LENGTH];
    Arrays.fill(b, (byte) 0xFF);
    return new UniqueID(b);
  }

  /**
   * Generate an UniqueID with random value.
   */
  public static UniqueID randomId() {
    byte[] b = new byte[LENGTH];
    new Random().nextBytes(b);
    return new UniqueID(b);
  }

  public UniqueID(byte[] id) {
    if (id.length != LENGTH) {
      throw new IllegalArgumentException("Illegal argument for UniqueID, expect " + LENGTH
          + " bytes, but got " + id.length + " bytes.");
    }

    this.id = id;
  }

  /**
   * Get the byte data of this UniqueID.
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
   * Create a copy of this UniqueID.
   */
  public UniqueID copy() {
    byte[] nid = Arrays.copyOf(id, id.length);
    return new UniqueID(nid);
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
}
