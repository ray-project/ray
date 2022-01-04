package io.ray.runtime.serialization;

public interface SerializerFactory {
  <T> Serializer<T> createSerializer(RaySerde raySerDe, Class<T> cls);
}
