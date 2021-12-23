package io.ray.serialization.resolver;

import java.util.List;

public interface ClassProvider {

  /** @return a class list for registration */
  List<Class<?>> getClasses();
}
