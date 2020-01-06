package org.ray.streaming.python.descriptor;

/**
 * Descriptor is used to describe all python-related function/operator/partition
 */
public interface Descriptor {

  /**
   * Serialize a descriptor using an opaque protocol so that
   * it can be deserialized in python.
   */
  byte[] toBytes();

}
