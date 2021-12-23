package io.ray.serialization.resolver;

import io.ray.serialization.Fury;
import io.ray.serialization.util.MemoryBuffer;

/** This class is used to track objects that have already been read or written. */
public interface ReferenceResolver {
  // TODO(mubai) add putIfAbsent for cuckoo hash.

  /** Returns an id for the object if it has been written previously, otherwise returns -1. */
  int getWriteRefId(Object object);

  /**
   * Returns a new id for an object that is being written for the first time. The id should be
   * consistent with {@link #nextReadRefId}
   */
  int addWriteObject(Object object);

  /**
   * Write reference and tag for the obj if the obj has been written previously, write null/not-null
   * tag otherwise.
   *
   * @return true if no bytes need to be written for the object.
   */
  boolean writeReferenceOrNull(MemoryBuffer buffer, Object obj);

  /**
   * Returns {@link Fury#NOT_NULL_REF} if a reference to a previously read object was read
   *
   * <p>Returns {@link Fury#NULL} if the object is null.
   *
   * <p>Returns {@link Fury#NOT_NULL} if the object is not null and reference tracking is not
   * enabled or the object is first read.
   */
  byte readReferenceOrNull(MemoryBuffer buffer);

  /**
   * Preserve a reference id, which is used by {@link #reference}/@link #setReadObject} to set up
   * reference for object that is first deserialized.
   *
   * @return a reference id or -1 if reference is not enabled.
   */
  int preserveReferenceId();

  /**
   * Call this method immediately after composited object such as object array/map/collection/bean
   * is created so that circular reference can be deserialized correctly.
   */
  void reference(Object object);

  /** Returns the object for the specified id. */
  Object getReadObject(int id);

  Object getReadObject();

  /**
   * Reserves the id for the next object that will be read. This is called only the first time an
   * object is encountered.
   */
  int nextReadRefId();

  /**
   * Sets the id for an object that has been read.
   *
   * @param id The id from {@link #nextReadRefId)}.
   * @param object the object that has been read
   */
  void setReadObject(int id, Object object);

  void reset();

  void resetWrite();

  void resetRead();
}
