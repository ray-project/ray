package io.ray.api.id;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

/** Represents a unique id of all Ray concepts, including workers, actors, checkpoints, etc. */
public class UniqueId extends BaseId implements Serializable {

  public static final int LENGTH = 28;
  public static final UniqueId NIL = genNil();

  /** Create a UniqueId from a hex string. */
  public static UniqueId fromHexString(String hex) {
    return new UniqueId(hexString2Bytes(hex));
  }

  /** Creates a UniqueId from a ByteBuffer. */
  public static UniqueId fromByteBuffer(ByteBuffer bb) {
    return new UniqueId(byteBuffer2Bytes(bb));
  }

  /** Creates a UniqueId from bytes */
  public static UniqueId fromBytes(byte[] bytes) {
    return new UniqueId(bytes);
  }

  /** Generate a nil UniqueId. */
  private static UniqueId genNil() {
    byte[] b = new byte[LENGTH];
    Arrays.fill(b, (byte) 0xFF);
    return new UniqueId(b);
  }

  /** Generate an UniqueId with random value. */
  public static UniqueId randomId() {
    byte[] b = new byte[LENGTH];
    new Random().nextBytes(b);
    return new UniqueId(b);
  }

  public UniqueId(byte[] id) {
    super(id);
  }

  @Override
  public int size() {
    return LENGTH;
  }
}
