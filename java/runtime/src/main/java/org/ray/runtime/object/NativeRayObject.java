package org.ray.runtime.object;

import com.google.common.base.Preconditions;

/**
 * Binary representation of ray object.
 */
public class NativeRayObject {

  public byte[] data;
  public byte[] metadata;

  public NativeRayObject(byte[] data, byte[] metadata) {
    Preconditions.checkNotNull(data);
    Preconditions.checkNotNull(metadata);
    this.data = data;
    this.metadata = metadata;
  }

  @Override
  public String toString() {
    return "<data>: " + data.length + ", <metadata>: " + metadata.length;
  }
}

