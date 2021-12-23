package io.ray.serialization.resolver;

import io.ray.serialization.Fury;
import io.ray.serialization.util.MemoryBuffer;

public final class NoReferenceResolver implements ReferenceResolver {
  @Override
  public int getWriteRefId(Object object) {
    return -1;
  }

  @Override
  public int addWriteObject(Object object) {
    return -1;
  }

  @Override
  public boolean writeReferenceOrNull(MemoryBuffer buffer, Object obj) {
    if (obj == null) {
      buffer.writeByte(Fury.NULL);
      return true;
    } else {
      buffer.writeByte(Fury.NOT_NULL);
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
    if (headFlag == Fury.NULL) {
      return Fury.NULL;
    } else {
      return Fury.NOT_NULL;
    }
  }

  @Override
  public void reset() {}

  @Override
  public void resetWrite() {}

  @Override
  public void resetRead() {}
}
