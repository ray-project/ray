package io.ray.runtime.serialization.serializers;

import io.ray.runtime.io.MemoryBuffer;
import io.ray.runtime.serialization.RaySerde;
import io.ray.runtime.serialization.resolver.ReferenceResolver;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/** All map serializers must extends {@link MapSerializer}. */
@SuppressWarnings({"unchecked", "rawtypes"})
public class MapSerializers {

  public static class MapSerializer<T extends Map> extends Serializer<T> {
    protected final boolean supportCodegenHook;
    private final ReferenceResolver referenceResolver;
    private Serializer keySerializer;
    private Serializer valueSerializer;

    public MapSerializer(RaySerde raySerDe, Class<T> cls) {
      this(raySerDe, cls, !JavaSerializers.isDynamicGeneratedCLass(cls));
    }

    public MapSerializer(RaySerde raySerDe, Class<T> cls, boolean supportCodegenHook) {
      super(raySerDe, cls);
      this.supportCodegenHook = supportCodegenHook;
      this.referenceResolver = raySerDe.getReferenceResolver();
    }

    public void setKeySerializer(Serializer keySerializer) {
      this.keySerializer = keySerializer;
    }

    public void setValueSerializer(Serializer valueSerializer) {
      this.valueSerializer = valueSerializer;
    }

    public void clearKeyValueSerializer() {
      keySerializer = null;
      valueSerializer = null;
    }

    @Override
    public void write(RaySerde raySerDe, MemoryBuffer buffer, T value) {
      buffer.writeInt(value.size());
      writeElements(raySerDe, buffer, value);
    }

    protected final void writeElements(RaySerde raySerDe, MemoryBuffer buffer, T map) {
      Serializer keySerializer = this.keySerializer;
      Serializer valueSerializer = this.valueSerializer;
      if (keySerializer != null && valueSerializer != null) {
        for (Object object : map.entrySet()) {
          Map.Entry entry = (Map.Entry) object;
          Object key = entry.getKey();
          Object value = entry.getValue();
          raySerDe.serializeReferencableToJava(buffer, key, keySerializer);
          raySerDe.serializeReferencableToJava(buffer, value, valueSerializer);
        }
      } else if (keySerializer != null) {
        for (Object object : map.entrySet()) {
          Map.Entry entry = (Map.Entry) object;
          raySerDe.serializeReferencableToJava(buffer, entry.getKey(), keySerializer);
          raySerDe.serializeReferencableToJava(buffer, entry.getValue());
        }
      } else if (valueSerializer != null) {
        for (Object object : map.entrySet()) {
          Map.Entry entry = (Map.Entry) object;
          raySerDe.serializeReferencableToJava(buffer, entry.getKey());
          raySerDe.serializeReferencableToJava(buffer, entry.getValue(), valueSerializer);
        }
      } else {
        for (Object object : map.entrySet()) {
          Map.Entry entry = (Map.Entry) object;
          raySerDe.serializeReferencableToJava(buffer, entry.getKey());
          raySerDe.serializeReferencableToJava(buffer, entry.getValue());
        }
      }
      // Restore the Serializer. the kvSerializer may be set to others if the nested key or value
      // serialization has map field.
      this.keySerializer = keySerializer;
      this.valueSerializer = keySerializer;
    }

    @Override
    public T read(RaySerde raySerDe, MemoryBuffer buffer, Class<T> type) {
      int size = buffer.readInt();
      T map = newInstance(type, size);
      readElements(raySerDe, buffer, size, map);
      return map;
    }

    protected final void readElements(RaySerde raySerDe, MemoryBuffer buffer, int size, T map) {
      Serializer keySerializer = this.keySerializer;
      Serializer valueSerializer = this.valueSerializer;
      if (keySerializer != null && valueSerializer != null) {
        for (int i = 0; i < size; i++) {
          Object key = raySerDe.deserializeReferencableFromJava(buffer, keySerializer);
          Object value = raySerDe.deserializeReferencableFromJava(buffer, valueSerializer);
          map.put(key, value);
        }
      } else if (keySerializer != null) {
        for (int i = 0; i < size; i++) {
          Object key = raySerDe.deserializeReferencableFromJava(buffer, keySerializer);
          map.put(key, raySerDe.deserializeReferencableFromJava(buffer));
        }
      } else if (valueSerializer != null) {
        Object key = raySerDe.deserializeReferencableFromJava(buffer);
        Object value = raySerDe.deserializeReferencableFromJava(buffer, valueSerializer);
        map.put(key, value);
      } else {
        for (int i = 0; i < size; i++) {
          Object key = raySerDe.deserializeReferencableFromJava(buffer);
          Object value = raySerDe.deserializeReferencableFromJava(buffer);
          map.put(key, value);
        }
      }
    }

    protected T newInstance(Class<T> cls, int size) {
      try {
        T t = cls.newInstance();
        raySerDe.getReferenceResolver().reference(t);
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
     * Write data except size and elements.
     *
     * <ol>
     *   In codegen, here is the call order:
     *   <li>write map class if not final
     *   <li>write map size
     *   <li>writeHeader
     *   <li>write keys/values
     * </ol>
     */
    public void writeHeader(RaySerde raySerDe, MemoryBuffer buffer, T value) {}

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
    public T newMap(RaySerde raySerDe, MemoryBuffer buffer, Class<T> type, int numElements) {
      return newInstance(type, numElements);
    }
  }

  public static final class HashMapSerializer extends MapSerializer<HashMap> {
    public HashMapSerializer(RaySerde raySerDe) {
      super(raySerDe, HashMap.class);
    }

    @Override
    protected HashMap newInstance(Class<HashMap> cls, int size) {
      HashMap hashMap = new HashMap(size);
      raySerDe.getReferenceResolver().reference(hashMap);
      return hashMap;
    }
  }

  public static class SortedMapSerializer<T extends SortedMap> extends MapSerializer<T> {
    private Constructor<?> constructor;

    public SortedMapSerializer(RaySerde raySerDe, Class<T> cls) {
      super(raySerDe, cls);
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
    public void write(RaySerde raySerDe, MemoryBuffer buffer, T value) {
      buffer.writeInt(value.size());
      raySerDe.serializeReferencableToJava(buffer, value.comparator());
      writeElements(raySerDe, buffer, value);
    }

    @Override
    public T read(RaySerde raySerDe, MemoryBuffer buffer, Class<T> type) {
      int len = buffer.readInt();
      T map = newMap(raySerDe, buffer, type, len);
      readElements(raySerDe, buffer, len, map);
      return map;
    }

    @Override
    public void writeHeader(RaySerde raySerDe, MemoryBuffer buffer, T value) {
      raySerDe.serializeReferencableToJava(buffer, value.comparator());
    }

    @Override
    public T newMap(RaySerde raySerDe, MemoryBuffer buffer, Class<T> type, int numElements) {
      T map;
      Comparator comparator = (Comparator) raySerDe.deserializeReferencableFromJava(buffer);
      if (type == TreeMap.class) {
        map = (T) new TreeMap(comparator);
      } else {
        try {
          map = (T) constructor.newInstance(comparator);
        } catch (IllegalAccessException | InvocationTargetException | InstantiationException e) {
          throw new RuntimeException(e);
        }
      }
      raySerDe.getReferenceResolver().reference(map);
      return map;
    }
  }

  public static final class MapDefaultJavaSerializer<T extends Map> extends MapSerializer<T> {
    private final DefaultSerializer<T> fallbackSerializer;

    public MapDefaultJavaSerializer(RaySerde raySerDe, Class<T> cls) {
      super(raySerDe, cls, false);
      fallbackSerializer = new DefaultSerializer<>(raySerDe, cls);
    }

    @Override
    public void write(RaySerde raySerDe, MemoryBuffer buffer, T value) {
      fallbackSerializer.write(raySerDe, buffer, value);
    }

    @Override
    public T read(RaySerde raySerDe, MemoryBuffer buffer, Class<T> type) {
      return fallbackSerializer.read(raySerDe, buffer, type);
    }
  }
}
