package io.ray.serialization.serializers;

import io.ray.serialization.Fury;
import io.ray.serialization.util.MemoryBuffer;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/** All map serializers must extends {@link MapSerializer} */
public class MapSerializers {

  public static class MapSerializer<T extends Map> extends Serializer<T> {
    protected final boolean supportCodegenHook;

    public MapSerializer(Fury fury, Class<T> cls) {
      this(fury, cls, !JavaSerializers.isDynamicGeneratedCLass(cls));
    }

    public MapSerializer(Fury fury, Class<T> cls, boolean supportCodegenHook) {
      super(fury, cls);
      this.supportCodegenHook = supportCodegenHook;
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, T value) {
      buffer.writeInt(value.size());
      writeElements(fury, buffer, value);
    }

    protected final void writeElements(Fury fury, MemoryBuffer buffer, T value) {
      for (Object object : value.entrySet()) {
        Map.Entry entry = (Map.Entry) object;
        fury.serializeReferencableToJava(buffer, entry.getKey());
        fury.serializeReferencableToJava(buffer, entry.getValue());
      }
    }

    @Override
    public T read(Fury fury, MemoryBuffer buffer, Class<T> type) {
      int size = buffer.readInt();
      T map = newInstance(type, size);
      readElements(fury, buffer, size, map);
      return map;
    }

    @SuppressWarnings("unchecked")
    protected final void readElements(Fury fury, MemoryBuffer buffer, int size, T map) {
      for (int i = 0; i < size; i++) {
        Object key = fury.deserializeReferencableFromJava(buffer);
        Object value = fury.deserializeReferencableFromJava(buffer);
        map.put(key, value);
      }
    }

    protected T newInstance(Class<T> cls, int size) {
      try {
        T t = cls.newInstance();
        fury.getReferenceResolver().reference(t);
        return t;
      } catch (InstantiationException | IllegalAccessException e) {
        throw new IllegalArgumentException("Please provide public no arguments constructor", e);
      }
    }

    /**
     * Hook for java serialization codegen, read/write key/value by entrySet.
     *
     * <p>For key/value type which is final, using codegen may get a big performance gain
     *
     * @return true if read/write key/value support calling entrySet method
     */
    public final boolean supportCodegenHook() {
      return supportCodegenHook;
    }

    /**
     * Write data except size and elements
     *
     * <ol>
     *   In codegen, follows is call order:
     *   <li>write map class if not final
     *   <li>write map size
     *   <li>writeHeader
     *   <li>write keys/values
     * </ol>
     */
    public void writeHeader(Fury fury, MemoryBuffer buffer, T value) {}

    /**
     * Read data except size and elements, return empty map to be filled.
     *
     * <ol>
     *   In codegen, follows is call order:
     *   <li>read map class if not final
     *   <li>read map size
     *   <li>newMap
     *   <li>read keys/values
     * </ol>
     */
    public T newMap(Fury fury, MemoryBuffer buffer, Class<T> type, int numElements) {
      return newInstance(type, numElements);
    }
  }

  public static final class HashMapSerializer extends MapSerializer<HashMap> {
    public HashMapSerializer(Fury fury) {
      super(fury, HashMap.class);
    }

    @Override
    protected HashMap newInstance(Class<HashMap> cls, int size) {
      HashMap hashMap = new HashMap(size);
      fury.getReferenceResolver().reference(hashMap);
      return hashMap;
    }
  }

  public static class SortedMapSerializer<T extends SortedMap> extends MapSerializer<T> {
    private Constructor<?> constructor;

    public SortedMapSerializer(Fury fury, Class<T> cls) {
      super(fury, cls);
      if (cls != TreeMap.class) {
        try {
          this.constructor = cls.getConstructor(Comparator.class);
          if (!constructor.isAccessible()) {
            constructor.setAccessible(true);
          }
        } catch (Exception e) {
          throw new UnsupportedOperationException(e);
        }
      }
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, T value) {
      buffer.writeInt(value.size());
      fury.serializeReferencableToJava(buffer, value.comparator());
      writeElements(fury, buffer, value);
    }

    @Override
    public T read(Fury fury, MemoryBuffer buffer, Class<T> type) {
      int len = buffer.readInt();
      T map = newMap(fury, buffer, type, len);
      readElements(fury, buffer, len, map);
      return map;
    }

    @Override
    public void writeHeader(Fury fury, MemoryBuffer buffer, T value) {
      fury.serializeReferencableToJava(buffer, value.comparator());
    }

    @SuppressWarnings("unchecked")
    @Override
    public T newMap(Fury fury, MemoryBuffer buffer, Class<T> type, int numElements) {
      T map;
      Comparator comparator = (Comparator) fury.deserializeReferencableFromJava(buffer);
      if (type == TreeMap.class) {
        map = (T) new TreeMap(comparator);
      } else {
        try {
          map = (T) constructor.newInstance(comparator);
        } catch (IllegalAccessException | InvocationTargetException | InstantiationException e) {
          throw new RuntimeException(e);
        }
      }
      fury.getReferenceResolver().reference(map);
      return map;
    }
  }

  public static final class MapDefaultJavaSerializer<T extends Map> extends MapSerializer<T> {
    private final boolean useCodegen;
    private final Serializer<T> codegenSerializer;
    private final FallbackSerializer<T> fallbackSerializer;

    public MapDefaultJavaSerializer(Fury fury, Class<T> cls) {
      super(fury, cls, false);
      useCodegen = CodegenSerializer.support(fury, cls);
      if (useCodegen) {
        codegenSerializer = fury.getClassResolver().getTypedSerializer(cls);
        fallbackSerializer = null;
      } else {
        fallbackSerializer = new FallbackSerializer<>(fury, cls);
        codegenSerializer = null;
      }
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, T value) {
      if (useCodegen) {
        codegenSerializer.write(fury, buffer, value);
      } else {
        fallbackSerializer.write(fury, buffer, value);
      }
    }

    @Override
    public T read(Fury fury, MemoryBuffer buffer, Class<T> type) {
      if (useCodegen) {
        return codegenSerializer.read(fury, buffer, type);
      } else {
        return fallbackSerializer.read(fury, buffer, type);
      }
    }
  }
}
