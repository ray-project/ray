package io.ray.runtime.serializer;

import io.ray.runtime.actor.NativeActorHandle;
import io.ray.runtime.actor.NativeActorHandleSerializer;
import org.nustaq.serialization.FSTConfiguration;

/**
 * Java object serialization TODO: use others (e.g. Arrow) for higher performance
 */
public class FstSerializer {

  private static final ThreadLocal<FSTConfiguration> conf = ThreadLocal.withInitial(() -> {
    FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();
    conf.registerSerializer(NativeActorHandle.class, new NativeActorHandleSerializer(), true);
    return conf;
  });


  public static byte[] encode(Object obj) {
    FSTConfiguration current = conf.get();
    current.setClassLoader(Thread.currentThread().getContextClassLoader());
    return current.asByteArray(obj);
  }


  @SuppressWarnings("unchecked")
  public static <T> T decode(byte[] bs) {
    FSTConfiguration current = conf.get();
    current.setClassLoader(Thread.currentThread().getContextClassLoader());
    return (T) current.asObject(bs);
  }
}
