package io.ray.api.id;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

public class PlacementGroupId extends BaseId implements Serializable {

  public static final int LENGTH = 16;

  public static final PlacementGroupId NIL = nil();

  private PlacementGroupId(byte[] id) {
    super(id);
  }

  public static PlacementGroupId fromByteBuffer(ByteBuffer bb) {
    return new PlacementGroupId(byteBuffer2Bytes(bb));
  }

  public static PlacementGroupId fromBytes(byte[] bytes) {
    return new PlacementGroupId(bytes);
  }

  /**
   * Generate a nil PlacementGroupId.
   */
  private static PlacementGroupId nil() {
    byte[] b = new byte[LENGTH];
    Arrays.fill(b, (byte) 0xFF);
    return new PlacementGroupId(b);
  }

  /**
   * Generate an PlacementGroupId with random value. Used for local mode and test only.
   */
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
