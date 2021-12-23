package io.ray.serialization.resolver;

import java.util.IdentityHashMap;

/**
 * A context is used to add some context-related information, so that the serializers can setup
 * relation between serializing different objects. The context will be reset after finished
 * serializing/deserializing the object tree.
 */
public final class SerializationContext {
  private IdentityHashMap<Object, Object> objects = new IdentityHashMap<>();

  /** Return the previous value associated with <tt>key</tt>, or <tt>null</tt>. */
  public Object add(Object key, Object value) {
    return objects.put(key, value);
  }

  public boolean containsKey(Object key) {
    return objects.containsKey(key);
  }

  public Object get(Object key) {
    return objects.get(key);
  }

  public void reset() {
    if (objects.size() > 0) {
      objects.clear();
    }
  }
}
