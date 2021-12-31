package io.ray.runtime.serialization.resolver;

import io.ray.runtime.io.MemoryBuffer;
import io.ray.runtime.serialization.RaySerde;

public final class NoReferenceResolver implements ReferenceResolver {

  @Override
  public boolean writeReferenceOrNull(MemoryBuffer buffer, Object obj) {
    if (obj == null) {
      buffer.writeByte(RaySerde.NULL);
      return true;
    } else {
      buffer.writeByte(RaySerde.NOT_NULL);
      return false;
    }
  }

  @Override
  public Object getReadObject(int id) {
    return null;
  }

  @Override
  public Object getReadObject() {
    return null;
  }

  @Override
  public int nextReadRefId() {
    return -1;
  }

  @Override
  public void setReadObject(int id, Object object) {}

  @Override
  public int preserveReferenceId() {
    return -1;
  }

  @Override
  public void reference(Object object) {}

  @Override
  public byte readReferenceOrNull(MemoryBuffer buffer) {
    byte headFlag = buffer.readByte();
    if (headFlag == RaySerde.NULL) {
      return RaySerde.NULL;
    } else {
      return RaySerde.NOT_NULL;
    }
  }

  @Override
  public void reset() {}

  @Override
  public void resetWrite() {}

  @Override
  public void resetRead() {}
}
