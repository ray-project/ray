package io.ray.runtime.serialization.serializers;

import io.ray.runtime.serialization.RaySerde;

public interface SerializerFactory {

  Serializer createSerializer(RaySerde raySerDe, Class<?> cls);
}
