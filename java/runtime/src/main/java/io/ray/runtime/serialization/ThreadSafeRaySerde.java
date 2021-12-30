package io.ray.runtime.serialization;

import com.google.common.base.Preconditions;
import io.ray.runtime.io.MemoryBuffer;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class ThreadSafeRaySerde {
  private final Set<RaySerde> identifySet = Collections.newSetFromMap(new IdentityHashMap<>());
  private final ThreadLocal<MemoryBuffer> bufferLocal =
      ThreadLocal.withInitial(() -> MemoryBuffer.newHeapBuffer(32));
  private final ThreadLocal<RaySerde> serdeLocal;

  /**
   * Configure serde in {@code serdeFactory}.
   *
   * @param supplier the supplier factory to be used to determine and configure the initial serde
   */
  public ThreadSafeRaySerde(Supplier<RaySerde> supplier) {
    serdeLocal =
        ThreadLocal.withInitial(
            () -> {
              RaySerde raySerDe = supplier.get();
              Preconditions.checkArgument(
                  !identifySet.contains(raySerDe), "Serde shouldn't be shared between threads");
              identifySet.add(raySerDe);
              return raySerDe;
            });
  }

  public byte[] serialize(Object obj) {
    MemoryBuffer buffer = bufferLocal.get();
    buffer.writerIndex(0);
    serdeLocal.get().serialize(buffer, obj);
    return buffer.getBytes(0, buffer.writerIndex());
  }

  public MemoryBuffer serialize(Object obj, MemoryBuffer buffer) {
    return serdeLocal.get().serialize(buffer, obj);
  }

  public Object deserialize(byte[] bytes) {
    return serdeLocal.get().deserialize(bytes);
  }

  public Object deserialize(long address, int size) {
    return serdeLocal.get().deserialize(address, size);
  }

  public Object deserialize(MemoryBuffer buffer) {
    return serdeLocal.get().deserialize(buffer);
  }

  public Object deserialize(ByteBuffer byteBuffer) {
    return serdeLocal.get().deserialize(MemoryBuffer.fromByteBuffer(byteBuffer));
  }
}
