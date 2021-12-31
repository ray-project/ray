package io.ray.runtime.serialization.serializers;

import io.ray.runtime.io.MemoryBuffer;
import io.ray.runtime.serialization.RaySerde;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.SortedSet;
import java.util.TreeSet;

/** All collection serializers must extend {@link CollectionSerializer}. */
@SuppressWarnings({"unchecked", "rawtypes"})
public class CollectionSerializers {
  public static class CollectionSerializer<T extends Collection> extends Serializer<T> {
    private final boolean supportCodegenHook;
    private Serializer elemSerializer;

    public CollectionSerializer(RaySerde raySerDe, Class<T> cls) {
      this(raySerDe, cls, !JavaSerializers.isDynamicGeneratedCLass(cls));
    }

    public CollectionSerializer(RaySerde raySerDe, Class<T> cls, boolean supportCodegenHook) {
      super(raySerDe, cls);
      this.supportCodegenHook = supportCodegenHook;
    }

    public void setElementSerializer(Serializer serializer) {
      elemSerializer = serializer;
    }

    public void clearElementSerializer() {
      elemSerializer = null;
    }

    @Override
    public void write(MemoryBuffer buffer, T value) {
      int len = value.size();
      buffer.writeInt(len);
      writeElements(raySerDe, buffer, value);
    }

    protected final void writeElements(RaySerde raySerDe, MemoryBuffer buffer, T value) {
      Serializer elemSerializer = this.elemSerializer;
      if (elemSerializer == null) {
        for (Object elem : value) {
          raySerDe.serializeReferencableToJava(buffer, elem);
        }
      } else {
        for (Object elem : value) {
          raySerDe.serializeReferencableToJava(buffer, elem, elemSerializer);
        }
        // Restore the elemSerializer. the elemSerializer may be set to others if the nested
        // serialization has collection field.
        this.elemSerializer = elemSerializer;
      }
    }

    @Override
    public T read(MemoryBuffer buffer) {
      int numElements = buffer.readInt();
      T collection = newInstance(cls, numElements);
      readElements(raySerDe, buffer, collection, numElements);
      return collection;
    }

    protected final void readElements(
        RaySerde raySerDe, MemoryBuffer buffer, T collection, int numElements) {
      Serializer elemSerializer = this.elemSerializer;
      if (elemSerializer == null) {
        for (int i = 0; i < numElements; i++) {
          Object elem = raySerDe.deserializeReferencableFromJava(buffer);
          collection.add(elem);
        }
      } else {
        for (int i = 0; i < numElements; i++) {
          collection.add(raySerDe.deserializeReferencableFromJava(buffer, elemSerializer));
        }
        this.elemSerializer = elemSerializer;
      }
    }

    protected T newInstance(Class<T> cls, int numElements) {
      try {
        T instance = cls.newInstance();
        raySerDe.getReferenceResolver().reference(instance);
        return instance;
      } catch (InstantiationException | IllegalAccessException e) {
        throw new IllegalArgumentException(
            "Please provide public no arguments constructor for class " + cls, e);
      }
    }

    /**
     * Hook for java serialization codegen, read/write elements will call collection.get/add
     * methods.
     *
     * <p>For key/value type which is final, using codegen may get a big performance gain
     *
     * @return true if read/write elements support calling collection.get/add methods
     */
    public final boolean supportCodegenHook() {
      return supportCodegenHook;
    }

    /**
     * Write data except size and elements.
     *
     * <ol>
     *   In codegen, here is the call order:
     *   <li>write collection class if not final
     *   <li>write collection size
     *   <li>writeHeader
     *   <li>write elements
     * </ol>
     */
    public void writeHeader(RaySerde raySerDe, MemoryBuffer buffer, T value) {}

    /**
     * Read data except size and elements, return empty collection to be filled.
     *
     * <ol>
     *   In codegen, follows is call order:
     *   <li>read collection class if not final
     *   <li>read collection size
     *   <li>newCollection
     *   <li>read elements
     * </ol>
     */
    public T newCollection(RaySerde raySerDe, MemoryBuffer buffer, Class<T> type, int numElements) {
      return newInstance(type, numElements);
    }
  }

  public static final class ArrayListSerializer extends CollectionSerializer<ArrayList> {
    public ArrayListSerializer(RaySerde raySerDe) {
      super(raySerDe, ArrayList.class);
    }

    @Override
    protected ArrayList newInstance(Class<ArrayList> cls, int numElements) {
      ArrayList arrayList = new ArrayList(numElements);
      raySerDe.getReferenceResolver().reference(arrayList);
      return arrayList;
    }
  }

  public static final class HashSetSerializer extends CollectionSerializer<HashSet> {
    public HashSetSerializer(RaySerde raySerDe) {
      super(raySerDe, HashSet.class);
    }

    @Override
    protected HashSet newInstance(Class<HashSet> cls, int numElements) {
      HashSet hashSet = new HashSet(numElements);
      raySerDe.getReferenceResolver().reference(hashSet);
      return hashSet;
    }
  }

  public static class SortedSetSerializer<T extends SortedSet> extends CollectionSerializer<T> {
    private Constructor<?> constructor;

    public SortedSetSerializer(RaySerde raySerDe, Class<T> cls) {
      super(raySerDe, cls);
      if (cls != TreeSet.class) {
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
    public void write(MemoryBuffer buffer, T value) {
      buffer.writeInt(value.size());
      raySerDe.serializeReferencableToJava(buffer, value.comparator());
      super.writeElements(raySerDe, buffer, value);
    }

    @Override
    public T read(MemoryBuffer buffer) {
      int numElements = buffer.readInt();
      T collection = newCollection(raySerDe, buffer, cls, numElements);
      readElements(raySerDe, buffer, collection, numElements);
      return collection;
    }

    @Override
    public void writeHeader(RaySerde raySerDe, MemoryBuffer buffer, T value) {
      raySerDe.serializeReferencableToJava(buffer, value.comparator());
    }

    @Override
    public T newCollection(RaySerde raySerDe, MemoryBuffer buffer, Class<T> type, int numElements) {
      T collection;
      Comparator comparator = (Comparator) raySerDe.deserializeReferencableFromJava(buffer);
      if (type == TreeSet.class) {
        collection = (T) new TreeSet(comparator);
      } else {
        try {
          collection = (T) constructor.newInstance(comparator);
        } catch (IllegalAccessException | InvocationTargetException | InstantiationException e) {
          throw new RuntimeException(e);
        }
      }
      raySerDe.getReferenceResolver().reference(collection);
      return collection;
    }
  }

  public static final class CollectionDefaultJavaSerializer<T extends Collection>
      extends CollectionSerializer<T> {
    private final DefaultSerializer<T> fallbackSerializer;

    public CollectionDefaultJavaSerializer(RaySerde raySerDe, Class<T> cls) {
      super(raySerDe, cls, false);
      fallbackSerializer = new DefaultSerializer<>(raySerDe, cls);
    }

    @Override
    public void write(MemoryBuffer buffer, T value) {
      fallbackSerializer.write(buffer, value);
    }

    @Override
    public T read(MemoryBuffer buffer) {
      return fallbackSerializer.read(buffer);
    }
  }
}
