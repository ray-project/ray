package io.ray.serialization.serializers;

import io.ray.serialization.Fury;

public interface SerializerFactory {

  Serializer createSerializer(Fury fury, Class<?> cls);
}
