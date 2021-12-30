package io.ray.runtime.serialization.serializers;

/**
 * If the callback returns false, the given buffer is out-of-band; otherwise the buffer is
 * serialized in-band, i.e. inside the serialized stream.
 */
@FunctionalInterface
public interface BufferCallback {

  boolean apply(SerializedObject object);
}
