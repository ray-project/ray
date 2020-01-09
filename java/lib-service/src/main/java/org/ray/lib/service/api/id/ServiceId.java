package org.ray.lib.service.api.id;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import org.ray.api.id.BaseId;
import org.ray.api.id.JobId;

public class ServiceId extends BaseId implements Serializable {

  private static final int UNIQUE_BYTES_LENGTH = 4;

  public static final int LENGTH = JobId.LENGTH + UNIQUE_BYTES_LENGTH;

  public static final ServiceId NIL = nil();

  private ServiceId(byte[] id) {
    super(id);
  }

  public static ServiceId fromByteBuffer(ByteBuffer bb) {
    return new ServiceId(byteBuffer2Bytes(bb));
  }

  public static ServiceId fromBytes(byte[] bytes) {
    return new ServiceId(bytes);
  }

  /**
   * Generate a nil ServiceId.
   */
  private static ServiceId nil() {
    byte[] b = new byte[LENGTH];
    Arrays.fill(b, (byte) 0xFF);
    return new ServiceId(b);
  }

  /**
   * Generate an ServiceId with random value. Used for local mode and test only.
   */
  public static ServiceId fromRandom() {
    byte[] b = new byte[LENGTH];
    new Random().nextBytes(b);
    return new ServiceId(b);
  }

  @Override
  public int size() {
    return LENGTH;
  }
}
