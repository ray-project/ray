package io.ray.runtime.object;

import com.google.common.base.Preconditions;

/**
 * Binary representation of a ray object. See `RayObject` class in C++ for details.
 */
public class NativeRayObject {

  public byte[] data;
  public byte[] metadata;

  public NativeRayObject(byte[] data, byte[] metadata) {
    Preconditions.checkState(bufferLength(data) > 0 || bufferLength(metadata) > 0);
    this.data = data;
    this.metadata = metadata;
  }

  private static int bufferLength(byte[] buffer) {
    if (buffer == null) {
      return 0;
    }
    return buffer.length;
  }

  @Override
  public String toString() {
    return "<data>: " + bufferLength(data) + ", <metadata>: " + bufferLength(metadata);
  }
}

