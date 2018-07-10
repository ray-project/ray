package org.ray.core;

import org.nustaq.serialization.FSTConfiguration;

/**
 * Java object serialization TODO: use others (e.g. Arrow) for higher performance
 */
public class Serializer {

  static final ThreadLocal<FSTConfiguration> conf = ThreadLocal.withInitial(
      FSTConfiguration::createDefaultConfiguration);

  public static byte[] encode(Object obj) {
    return conf.get().asByteArray(obj);
  }

  public static byte[] encode(Object obj, ClassLoader classLoader) {
    byte[] result;
    FSTConfiguration current = conf.get();
    if (classLoader != null && classLoader != current.getClassLoader()) {
      ClassLoader old = current.getClassLoader();
      current.setClassLoader(classLoader);
      result = current.asByteArray(obj);
      current.setClassLoader(old);
    } else {
      result = current.asByteArray(obj);
    }

    return result;
  }

  @SuppressWarnings("unchecked")
  public static <T> T decode(byte[] bs) {
    return (T) conf.get().asObject(bs);
  }

  @SuppressWarnings("unchecked")
  public static <T> T decode(byte[] bs, ClassLoader classLoader) {
    Object object;
    FSTConfiguration current = conf.get();
    if (classLoader != null && classLoader != current.getClassLoader()) {
      ClassLoader old = current.getClassLoader();
      current.setClassLoader(classLoader);
      object = current.asObject(bs);
      current.setClassLoader(old);
    } else {
      object = current.asObject(bs);
    }
    return (T) object;
  }

  public static void setClassloader(ClassLoader classLoader) {
    conf.get().setClassLoader(classLoader);
  }
}
