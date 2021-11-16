package io.ray.serve.util;

import java.util.Collection;

public class CollectionUtil {

  public static <E> boolean isEmpty(Collection<E> collection) {
    return collection == null || collection.isEmpty();
  }
}
