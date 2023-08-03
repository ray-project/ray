package io.ray.api.id;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/** Represents the id of a Ray job. */
public class JobId extends BaseId implements Serializable {

  public static final int LENGTH = 4;

  public static final JobId NIL = genNil();

  /** Create a JobID instance according to the given bytes. */
  private JobId(byte[] id) {
    super(id);
  }

  /** Create a JobId from byte array. */
  public static JobId fromBytes(byte[] bytes) {
    return new JobId(bytes);
  }

  /** Create a JobId from a given hex string. */
  public static JobId fromHexString(String hex) {
    return new JobId(hexString2Bytes(hex));
  }

  /** Creates a JobId from the given ByteBuffer. */
  public static JobId fromByteBuffer(ByteBuffer bb) {
    return new JobId(byteBuffer2Bytes(bb));
  }

  public static JobId fromInt(int value) {
    if (value > Math.pow(256, JobId.LENGTH)) {
      throw new IllegalArgumentException(
          "The integer value is invalid for a JobId. Value: " + value);
    }
    byte[] bytes = new byte[Integer.BYTES];
    ByteBuffer wbb = ByteBuffer.wrap(bytes);
    wbb.order(ByteOrder.LITTLE_ENDIAN);
    wbb.putInt(value);
    wbb.flip();
    wbb.limit(JobId.LENGTH);
    return JobId.fromByteBuffer(wbb);
  }

  /** Generate a nil JobId. */
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
