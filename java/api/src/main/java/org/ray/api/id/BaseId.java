package org.ray.api.id;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.xml.bind.DatatypeConverter;

public abstract class BaseId implements Serializable {
  private static final long serialVersionUID = 8588849129675565761L;
  private final byte[] id;

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
   * Create a BaseId instance according to the input byte array.
   */
  public BaseId(byte[] id) {
    if (id.length != size()) {
      throw new IllegalArgumentException("Illegal argument for Construct BaseId, expect " + size()
          + " bytes, but got " + id.length + " bytes.");
    }
    this.id = id;
  }

  /**
   * @return True if this id is nil.
   */
  public boolean isNil() {
    for (int i = 0; i < size(); ++i) {
      if (id[i] != (byte) 0xff) {
        return false;
      }
    }
    return true;
  }

  /**
   * Derived class should implement this function.
   * @return The length of this id in bytes.
   */
  public abstract int size();

  @Override
  public int hashCode() {
    return Arrays.hashCode(id);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }

    if (!this.getClass().equals(obj.getClass()) {
      return false;
    }

    BaseId r = (BaseId) obj;
    return Arrays.equals(id, r.id);
  }

  @Override
  public String toString() {
    return DatatypeConverter.printHexBinary(id).toLowerCase();
  }

  public static byte[] hexString2Bytes(String hex) {
    return DatatypeConverter.parseHexBinary(hex);
  }

  public static byte[] byteBuffer2Bytes(ByteBuffer bb) {
    byte[] id = new byte[bb.remaining()];
    bb.get(id);
    return id;
  }

}
