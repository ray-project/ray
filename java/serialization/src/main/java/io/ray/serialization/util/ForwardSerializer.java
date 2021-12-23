package io.ray.serialization.util;

import com.google.common.base.Preconditions;
import io.ray.serialization.Fury;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.function.Consumer;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A thread-safe serializer used to forward serialization to different serializer implementation.
 */
@ThreadSafe
@SuppressWarnings("unchecked")
public class ForwardSerializer {

  public abstract static class SerializerProxy<T> {
    /** Register custom serializers should be done in this method. */
    protected abstract T newSerializer();

    protected void setClassLoader(T serializer, ClassLoader classLoader) {
      throw new UnsupportedOperationException();
    }

    protected void register(T serializer, Class<?> clz) {
      throw new UnsupportedOperationException();
    }

    protected void register(T serializer, Class<?> clz, int id) {
      throw new UnsupportedOperationException();
    }

    protected abstract byte[] serialize(T serializer, Object obj);

    protected MemoryBuffer serialize(T serializer, Object obj, MemoryBuffer buffer) {
      byte[] bytes = serialize(serializer, obj);
      buffer.writeBytes(bytes);
      return buffer;
    }

    protected ByteBuffer serialize(T serializer, Object obj, ByteBuffer buffer) {
      byte[] bytes = serialize(serializer, obj);
      buffer.put(bytes);
      return buffer;
    }

    protected abstract Object deserialize(T serializer, byte[] bytes);

    protected Object deserialize(T serializer, long address, int size) {
      byte[] bytes = new byte[size];
      Platform.copyMemory(null, address, bytes, Platform.BYTE_ARRAY_OFFSET, size);
      return deserialize(serializer, bytes);
    }

    protected Object deserialize(T serializer, ByteBuffer byteBuffer) {
      return deserialize(serializer, MemoryUtils.wrap(byteBuffer));
    }

    protected Object deserialize(T serializer, MemoryBuffer buffer) {
      byte[] bytes = buffer.getRemainingBytes();
      return deserialize(serializer, bytes);
    }

    protected ClassLoader getClassLoader(T serializer) {
      throw new UnsupportedOperationException();
    }
  }

  public static class DefaultFuryProxy extends SerializerProxy<DefaultFuryProxy.LoaderBinding> {

    private final ThreadLocal<MemoryBuffer> bufferLocal =
        ThreadLocal.withInitial(() -> MemoryUtils.buffer(32));

    private final class LoaderBinding {
      private IdentityHashMap<ClassLoader, Fury> furyMap = new IdentityHashMap<>();
      private Consumer<Fury> bindingCallback = f -> {};
      private ClassLoader loader;
      private Fury fury;

      Fury get() {
        return fury;
      }

      public ClassLoader getClassLoader() {
        return loader;
      }

      void setClassLoader(ClassLoader loader) {
        if (this.loader != loader) {
          this.loader = loader;
          Fury fury = furyMap.get(loader);
          if (fury == null) {
            fury = newFurySerializer(loader);
            bindingCallback.accept(fury);
            furyMap.put(loader, fury);
            this.fury = fury;
          }
        }
      }
    }

    /** Override this method to register custom serializers. */
    @Override
    protected LoaderBinding newSerializer() {
      LoaderBinding loaderBinding = new LoaderBinding();
      loaderBinding.setClassLoader(Thread.currentThread().getContextClassLoader());
      return loaderBinding;
    }

    protected Fury newFurySerializer(ClassLoader loader) {
      return Fury.builder().withReferenceTracking(true).withClassLoader(loader).build();
    }

    @Override
    protected void setClassLoader(LoaderBinding binding, ClassLoader classLoader) {
      binding.setClassLoader(classLoader);
    }

    @Override
    protected ClassLoader getClassLoader(LoaderBinding binding) {
      return binding.getClassLoader();
    }

    @Override
    protected void register(LoaderBinding binding, Class<?> clz) {
      binding.furyMap.values().forEach(fury -> fury.register(clz));
      binding.bindingCallback =
          binding.bindingCallback.andThen(
              fury -> {
                fury.register(clz);
              });
    }

    @Override
    protected void register(LoaderBinding binding, Class<?> clz, int id) {
      Preconditions.checkArgument(id < Short.MAX_VALUE);
      binding.furyMap.values().forEach(fury -> fury.register(clz, (short) id));
      binding.bindingCallback =
          binding.bindingCallback.andThen(
              fury -> {
                fury.register(clz, (short) id);
              });
    }

    @Override
    protected byte[] serialize(LoaderBinding binding, Object obj) {
      MemoryBuffer buffer = bufferLocal.get();
      buffer.writerIndex(0);
      binding.get().serialize(buffer, obj);
      return buffer.getBytes(0, buffer.writerIndex());
    }

    @Override
    protected MemoryBuffer serialize(LoaderBinding binding, Object obj, MemoryBuffer buffer) {
      binding.get().serialize(buffer, obj);
      return buffer;
    }

    @Override
    protected Object deserialize(LoaderBinding serializer, byte[] bytes) {
      return serializer.get().deserialize(bytes);
    }

    @Override
    protected Object deserialize(LoaderBinding serializer, MemoryBuffer buffer) {
      return serializer.get().deserialize(buffer);
    }
  }

  private final SerializerProxy proxy;
  private final ThreadLocal<Object> serializerLocal;
  private Set<Object> serializerSet = Collections.newSetFromMap(new IdentityHashMap<>());
  private Consumer<Object> serializerCallback = obj -> {};

  public ForwardSerializer(SerializerProxy proxy) {
    this.proxy = proxy;
    serializerLocal =
        ThreadLocal.withInitial(
            () -> {
              Object serializer = proxy.newSerializer();
              synchronized (ForwardSerializer.this) {
                serializerCallback.accept(serializer);
              }
              serializerSet.add(serializer);
              return serializer;
            });
  }

  /** Set classLoader of serializer for current thread only. */
  public void setClassLoader(ClassLoader classLoader) {
    proxy.setClassLoader(serializerLocal.get(), classLoader);
  }

  /** @return classLoader of serializer for current thread. */
  public ClassLoader getClassLoader() {
    return proxy.getClassLoader(serializerLocal.get());
  }

  public synchronized void register(Class<?> clz) {
    serializerSet.forEach(serializer -> proxy.register(serializer, clz));
    serializerCallback =
        serializerCallback.andThen(
            serializer -> {
              proxy.register(serializer, clz);
            });
  }

  public synchronized void register(Class<?> clz, int id) {
    serializerSet.forEach(serializer -> proxy.register(serializer, clz, id));
    serializerCallback =
        serializerCallback.andThen(
            serializer -> {
              proxy.register(serializer, clz, id);
            });
  }

  public byte[] serialize(Object obj) {
    return proxy.serialize(serializerLocal.get(), obj);
  }

  public MemoryBuffer serialize(Object obj, MemoryBuffer buffer) {
    return proxy.serialize(serializerLocal.get(), obj, buffer);
  }

  public ByteBuffer serialize(Object obj, ByteBuffer buffer) {
    return proxy.serialize(serializerLocal.get(), obj, buffer);
  }

  public <T> T deserialize(byte[] bytes) {
    return (T) proxy.deserialize(serializerLocal.get(), bytes);
  }

  public <T> T deserialize(long address, int size) {
    return (T) proxy.deserialize(serializerLocal.get(), address, size);
  }

  public <T> T deserialize(ByteBuffer byteBuffer) {
    return (T) proxy.deserialize(serializerLocal.get(), byteBuffer);
  }

  public <T> T deserialize(MemoryBuffer buffer) {
    return (T) proxy.deserialize(serializerLocal.get(), buffer);
  }
}
