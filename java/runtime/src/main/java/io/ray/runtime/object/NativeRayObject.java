package io.ray.runtime.object;

import com.google.common.base.Preconditions;
import io.ray.api.id.BaseId;
import io.ray.api.id.ObjectId;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** Binary representation of a ray object. See `RayObject` class in C++ for details. */
public class NativeRayObject {

  public byte[] data;
  public byte[] metadata;
  // buffer is not null only if the object is arrow data
  public ByteBuffer buffer;
  public List<byte[]> containedObjectIds;

  public NativeRayObject(byte[] data, byte[] metadata) {
    Preconditions.checkState(bufferLength(data) > 0 || bufferLength(metadata) > 0);
    this.data = data;
    this.metadata = metadata;
    this.containedObjectIds = Collections.emptyList();
  }

  public NativeRayObject(ByteBuffer buffer, byte[] metadata) {
    Preconditions.checkState((buffer == null ? 0 : buffer.remaining()) > 0 || bufferLength(metadata) > 0);
    this.buffer = buffer;
    this.metadata = metadata;
    this.containedObjectIds = Collections.emptyList();
  }

  public void setContainedObjectIds(List<ObjectId> containedObjectIds) {
    this.containedObjectIds = toBinaryList(containedObjectIds);
  }

  private static int bufferLength(byte[] buffer) {
    if (buffer == null) {
      return 0;
    }
    return buffer.length;
  }

  private static List<byte[]> toBinaryList(List<ObjectId> ids) {
    return ids.stream().map(BaseId::getBytes).collect(Collectors.toList());
  }

  @Override
  public String toString() {
    return "<data>: " + bufferLength(data) + ", <metadata>: " + bufferLength(metadata);
  }
}
