package io.ray.serialization;

import com.google.common.base.Preconditions;
import io.ray.serialization.util.MemoryBuffer;
import io.ray.serialization.util.MemoryUtils;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class ThreadSafeFury {
  private Set<Fury> identifySet = Collections.newSetFromMap(new IdentityHashMap<>());
  private final ThreadLocal<MemoryBuffer> bufferLocal =
      ThreadLocal.withInitial(() -> io.ray.serialization.util.MemoryUtils.buffer(32));
  private final ThreadLocal<Fury> furyLocal;

  /**
   * Configure fury in {@code furyFactory}
   *
   * @param supplier the supplier factory to be used to determine and configure the initial fury
   */
  public ThreadSafeFury(Supplier<Fury> supplier) {
    furyLocal =
        ThreadLocal.withInitial(
            () -> {
              Fury fury = supplier.get();
              Preconditions.checkArgument(
                  !identifySet.contains(fury), "Fury shouldn't be shared between threads");
              identifySet.add(fury);
              return fury;
            });
  }

  public byte[] serialize(Object obj) {
    MemoryBuffer buffer = bufferLocal.get();
    buffer.writerIndex(0);
    furyLocal.get().serialize(buffer, obj);
    return buffer.getBytes(0, buffer.writerIndex());
  }

  public MemoryBuffer serialize(Object obj, MemoryBuffer buffer) {
    return furyLocal.get().serialize(buffer, obj);
  }

  public Object deserialize(byte[] bytes) {
    return furyLocal.get().deserialize(bytes);
  }

  public Object deserialize(long address, int size) {
    return furyLocal.get().deserialize(address, size);
  }

  public Object deserialize(MemoryBuffer buffer) {
    return furyLocal.get().deserialize(buffer);
  }

  public Object deserialize(ByteBuffer byteBuffer) {
    return furyLocal.get().deserialize(MemoryUtils.wrap(byteBuffer));
  }
}
