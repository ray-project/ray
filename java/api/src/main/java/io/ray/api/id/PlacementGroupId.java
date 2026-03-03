package io.ray.api.id;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

/** Represents the id of a placement group. */
public class PlacementGroupId extends BaseId implements Serializable {

  private static final int UNIQUE_BYTES_LENGTH = 14;

  public static final int LENGTH = JobId.LENGTH + UNIQUE_BYTES_LENGTH;

  public static final PlacementGroupId NIL = nil();

  private PlacementGroupId(byte[] id) {
    super(id);
  }

  /** Creates a PlacementGroupId from the given ByteBuffer. */
  public static PlacementGroupId fromByteBuffer(ByteBuffer bb) {
    return new PlacementGroupId(byteBuffer2Bytes(bb));
  }

  /** Create a PlacementGroupId instance according to the given bytes. */
  public static PlacementGroupId fromBytes(byte[] bytes) {
    return new PlacementGroupId(bytes);
  }

  /** Generate a nil PlacementGroupId. */
  private static PlacementGroupId nil() {
    byte[] b = new byte[LENGTH];
    Arrays.fill(b, (byte) 0xFF);
    return new PlacementGroupId(b);
  }

  /** Generate an PlacementGroupId with random value. Used for local mode and test only. */
  public static PlacementGroupId fromRandom() {
    byte[] b = new byte[LENGTH];
    new Random().nextBytes(b);
    return new PlacementGroupId(b);
  }

  @Override
  public int size() {
    return LENGTH;
  }
}
