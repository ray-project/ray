package io.ray.serialization.resolver;

import io.ray.serialization.Fury;
import io.ray.serialization.util.IntArray;
import io.ray.serialization.util.MemoryBuffer;
import java.util.ArrayList;
import java.util.IdentityHashMap;

public final class MapReferenceResolver implements ReferenceResolver {
  private final Fury fury;
  // TODO use IdentityObjectIntMap to avoid box and reduce hash lookup.
  private final IdentityHashMap<Object, Integer> writtenObjects = new IdentityHashMap<>();
  private final ArrayList<Object> readObjects = new ArrayList<>();
  private final IntArray readReferenceIds = new IntArray(0);

  // last read object which is not a reference
  private Object readObject;

  public MapReferenceResolver(Fury fury) {
    this.fury = fury;
  }

  @Override
  public int getWriteRefId(Object object) {
    Integer value = writtenObjects.get(object);
    if (value == null) {
      return -1;
    } else {
      return value;
    }
  }

  @Override
  public int addWriteObject(Object object) {
    int id = writtenObjects.size();
    writtenObjects.put(object, id);
    return id;
  }

  @Override
  public boolean writeReferenceOrNull(MemoryBuffer buffer, Object obj) {
    if (obj == null) {
      buffer.writeByte(Fury.NULL);
      return true;
    } else {
      int writtenId = getWriteRefId(obj);
      // The obj has been written previously.
      if (writtenId != -1) {
        buffer.writeByte(Fury.NOT_NULL_REF);
        buffer.writeInt(writtenId);
        return true;
      } else {
        addWriteObject(obj);
      }
      buffer.writeByte(Fury.NOT_NULL);
      return false;
    }
  }

  @Override
  public Object getReadObject(int id) {
    return readObjects.get(id);
  }

  @Override
  public Object getReadObject() {
    return readObject;
  }

  @Override
  public int nextReadRefId() {
    int id = readObjects.size();
    readObjects.add(null);
    return id;
  }

  @Override
  public void setReadObject(int id, Object object) {
    readObjects.set(id, object);
  }

  @Override
  public int preserveReferenceId() {
    int nextReadRefId = nextReadRefId();
    readReferenceIds.add(nextReadRefId);
    return nextReadRefId;
  }

  @Override
  public void reference(Object object) {
    int refId = readReferenceIds.pop();
    setReadObject(refId, object);
  }

  /**
   * Returns {@link Fury#NOT_NULL_REF} if a reference to a previously read object was read, which is
   * stored in {@link #readObject}.
   *
   * <p>Returns {@link Fury#NULL} if the object is null and set {@link #readObject} to null.
   *
   * <p>Returns {@link Fury#NOT_NULL} if the object is not null and the object is first read.
   */
  @Override
  public byte readReferenceOrNull(MemoryBuffer buffer) {
    byte headFlag = buffer.readByte();
    if (headFlag == Fury.NULL) {
      readObject = null;
      return Fury.NULL;
    } else {
      if (headFlag == Fury.NOT_NULL_REF) {
        // read reference id and get object from reference resolver
        int referenceId = buffer.readInt();
        readObject = getReadObject(referenceId);
        return Fury.NOT_NULL_REF;
      } else {
        readObject = null;
        return Fury.NOT_NULL;
      }
    }
  }

  @Override
  public void reset() {
    resetWrite();
    resetRead();
  }

  @Override
  public void resetWrite() {
    writtenObjects.clear();
  }

  @Override
  public void resetRead() {
    readObjects.clear();
    readReferenceIds.clear();
    readObject = null;
  }
}
