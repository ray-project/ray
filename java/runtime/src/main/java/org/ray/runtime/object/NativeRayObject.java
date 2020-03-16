package org.ray.runtime.object;

import com.google.common.base.Preconditions;
import org.ray.api.id.ObjectId;

import java.util.Collections;
import java.util.List;

/**
 * Binary representation of a ray object. See `RayObject` class in C++ for details.
 */
public class NativeRayObject {

  public byte[] data;
  public byte[] metadata;
  private List<ObjectId> containedObjectIds = Collections.emptyList();

  public NativeRayObject(byte[] data, byte[] metadata) {
    Preconditions.checkState(bufferLength(data) > 0 || bufferLength(metadata) > 0);
    this.data = data;
    this.metadata = metadata;
  }

  public List<ObjectId> getContainedObjectIds() {
    return containedObjectIds;
  }

  public void setContainedObjectIds(List<ObjectId> containedObjectIds) {
    this.containedObjectIds = containedObjectIds;
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

