package io.ray.serialization.encoder;

import io.ray.serialization.Fury;
import io.ray.serialization.serializers.Serializer;

public interface Generated {

  /**
   * Since janino doesn't support generics, we use {@link Object} to represent object type rather
   * generic type.
   */
  abstract class GeneratedSerializer extends Serializer implements Generated {
    public GeneratedSerializer(Fury fury, Class cls) {
      super(fury, cls);
    }
  }
}
