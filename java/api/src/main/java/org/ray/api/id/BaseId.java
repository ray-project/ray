package org.ray.api.id;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.xml.bind.DatatypeConverter;

public abstract class BaseId implements Serializable {
  private static final long serialVersionUID = 8588849129675565761L;
  private final byte[] id;
  private int hashCodeCache = 0;
  private Boolean isNilCache = null;

  /**
   * Create a BaseId instance according to the input byte array.
   */
  protected BaseId(byte[] id) {
    if (id.length != size()) {
      throw new IllegalArgumentException("Failed to construct BaseId, expect " + size()
              + " bytes, but got " + id.length + " bytes.");
    }
    this.id = id;
  }

  /**
   * Get the byte data of this id.
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
   * @return True if this id is nil.
   */
  public boolean isNil() {
    if (isNilCache == null) {
      boolean localIsNil = true;
      for (int i = 0; i < size(); ++i) {
        if (id[i] != (byte) 0xff) {
          localIsNil = false;
          break;
        }
      }
      isNilCache = localIsNil;
    }
    return isNilCache;
  }

  /**
   * Derived class should implement this function.
   * @return The length of this id in bytes.
   */
  public abstract int size();

  @Override
  public int hashCode() {
    // Lazy evaluation.
    if (hashCodeCache == 0) {
      hashCodeCache = Arrays.hashCode(id);
    }
    return hashCodeCache;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }

    if (!this.getClass().equals(obj.getClass())) {
      return false;
    }

    BaseId r = (BaseId) obj;
    return Arrays.equals(id, r.id);
  }

  @Override
  public String toString() {
    return DatatypeConverter.printHexBinary(id).toLowerCase();
  }

  protected static byte[] hexString2Bytes(String hex) {
    return DatatypeConverter.parseHexBinary(hex);
  }

  protected static byte[] byteBuffer2Bytes(ByteBuffer bb) {
    byte[] id = new byte[bb.remaining()];
    bb.get(id);
    return id;
  }

}
