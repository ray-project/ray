package org.ray.api.id;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;

/**
 * Represents the id of a Ray job.
 */
public class JobId extends BaseId implements Serializable {
  public static final int LENGTH = 4;
  public static final JobId NIL = genNil();

  /**
   * Constructor
   */
  private JobId(byte[] id) {
    super(id);
  }

  /**
   * Create a JobId from a given hex string.
   */
  public static JobId fromHexString(String hex) {
    return new JobId(hexString2Bytes(hex));
  }

  /**
   * Creates a JobId from the given ByteBuffer.
   */
  public static JobId fromByteBuffer(ByteBuffer bb) {
    return new JobId(byteBuffer2Bytes(bb));
  }

  public static JobId fromLong(Long num) {
    byte[] bytes = new byte[JobId.LENGTH];
    ByteBuffer wbb = ByteBuffer.wrap(bytes);
    wbb.order(ByteOrder.LITTLE_ENDIAN);
    wbb.putInt(num.intValue());
    return new JobId(bytes);
  }

  /**
   * generate a nil JobId.
   */
  private static JobId genNil() {
    byte[] b = new byte[LENGTH];
    Arrays.fill(b, (byte) 0xFF);
    return new JobId(b);
  }

  /**
   *
   * @return
   */
  // TODO(qwang): Remove this method.
  public static JobId randomId() {
    byte[] b = new byte[LENGTH];
    new Random().nextBytes(b);
    return new JobId(b);
  }

  @Override
  public int size() {
    return LENGTH;
  }
}
