package org.ray.api.id;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Represents the id of a Ray job.
 */
public class JobId extends BaseId implements Serializable {

  // Note that the max value of a job id is NIL which value is (2^32 - 1).
  public static final Long MAX_VALUE = (long) Math.pow(2, 32) - 1;

  public static final int LENGTH = 4;

  public static final JobId NIL = genNil();

  /**
   * Create a JobID instance according to the given bytes.
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

  public static JobId fromInt(int value) {
    byte[] bytes = new byte[JobId.LENGTH];
    ByteBuffer wbb = ByteBuffer.wrap(bytes);
    wbb.order(ByteOrder.LITTLE_ENDIAN);
    wbb.putInt(value);
    return new JobId(bytes);
  }

  /**
   * Generate a nil JobId.
   */
  private static JobId genNil() {
    byte[] b = new byte[LENGTH];
    Arrays.fill(b, (byte) 0xFF);
    return new JobId(b);
  }

  @Override
  public int size() {
    return LENGTH;
  }
}
