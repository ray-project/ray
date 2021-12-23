package io.ray.serialization.serializers;

import io.ray.serialization.Fury;
import io.ray.serialization.util.MemoryBuffer;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.SortedSet;
import java.util.TreeSet;

/** All collection serializers must extend {@link CollectionSerializer} */
public class CollectionSerializers {

  public static class CollectionSerializer<T extends Collection> extends Serializer<T> {
    private final boolean supportCodegenHook;

    public CollectionSerializer(Fury fury, Class<T> cls) {
      this(fury, cls, !JavaSerializers.isDynamicGeneratedCLass(cls));
    }

    public CollectionSerializer(Fury fury, Class<T> cls, boolean supportCodegenHook) {
      super(fury, cls);
      this.supportCodegenHook = supportCodegenHook;
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, T value) {
      int len = value.size();
      buffer.writeInt(len);
      writeElements(fury, buffer, value);
    }

    protected final void writeElements(Fury fury, MemoryBuffer buffer, T value) {
      for (Object elem : value) {
        fury.serializeReferencableToJava(buffer, elem);
      }
    }

    @Override
    public T read(Fury fury, MemoryBuffer buffer, Class<T> type) {
      int numElements = buffer.readInt();
      T collection = newInstance(type, numElements);
      readElements(fury, buffer, collection, numElements);
      return collection;
    }

    @SuppressWarnings("unchecked")
    protected final void readElements(
        Fury fury, MemoryBuffer buffer, T collection, int numElements) {
      for (int i = 0; i < numElements; i++) {
        Object elem = fury.deserializeReferencableFromJava(buffer);
        collection.add(elem);
      }
    }

    protected T newInstance(Class<T> cls, int numElements) {
      try {
        T instance = cls.newInstance();
        fury.getReferenceResolver().reference(instance);
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
     * Write data except size and elements
     *
     * <ol>
     *   In codegen, follows is call order:
     *   <li>write collection class if not final
     *   <li>write collection size
     *   <li>writeHeader
     *   <li>write elements
     * </ol>
     */
    public void writeHeader(Fury fury, MemoryBuffer buffer, T value) {}

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
    public T newCollection(Fury fury, MemoryBuffer buffer, Class<T> type, int numElements) {
      return newInstance(type, numElements);
    }
  }

  public static final class ArrayListSerializer extends CollectionSerializer<ArrayList> {
    public ArrayListSerializer(Fury fury) {
      super(fury, ArrayList.class);
    }

    @Override
    protected ArrayList newInstance(Class<ArrayList> cls, int numElements) {
      ArrayList arrayList = new ArrayList(numElements);
      fury.getReferenceResolver().reference(arrayList);
      return arrayList;
    }
  }

  public static final class HashSetSerializer extends CollectionSerializer<HashSet> {
    public HashSetSerializer(Fury fury) {
      super(fury, HashSet.class);
    }

    @Override
    protected HashSet newInstance(Class<HashSet> cls, int numElements) {
      HashSet hashSet = new HashSet(numElements);
      fury.getReferenceResolver().reference(hashSet);
      return hashSet;
    }
  }

  public static class SortedSetSerializer<T extends SortedSet> extends CollectionSerializer<T> {
    private Constructor<?> constructor;

    public SortedSetSerializer(Fury fury, Class<T> cls) {
      super(fury, cls);
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
    public void write(Fury fury, MemoryBuffer buffer, T value) {
      buffer.writeInt(value.size());
      fury.serializeReferencableToJava(buffer, value.comparator());
      super.writeElements(fury, buffer, value);
    }

    @Override
    public T read(Fury fury, MemoryBuffer buffer, Class<T> type) {
      int numElements = buffer.readInt();
      T collection = newCollection(fury, buffer, type, numElements);
      readElements(fury, buffer, collection, numElements);
      return collection;
    }

    @Override
    public void writeHeader(Fury fury, MemoryBuffer buffer, T value) {
      fury.serializeReferencableToJava(buffer, value.comparator());
    }

    @SuppressWarnings("unchecked")
    @Override
    public T newCollection(Fury fury, MemoryBuffer buffer, Class<T> type, int numElements) {
      T collection;
      Comparator comparator = (Comparator) fury.deserializeReferencableFromJava(buffer);
      if (type == TreeSet.class) {
        collection = (T) new TreeSet(comparator);
      } else {
        try {
          collection = (T) constructor.newInstance(comparator);
        } catch (IllegalAccessException | InvocationTargetException | InstantiationException e) {
          throw new RuntimeException(e);
        }
      }
      fury.getReferenceResolver().reference(collection);
      return collection;
    }
  }

  public static final class CollectionDefaultJavaSerializer<T extends Collection>
      extends CollectionSerializer<T> {
    private final DefaultSerializer<T> fallbackSerializer;

    public CollectionDefaultJavaSerializer(Fury fury, Class<T> cls) {
      super(fury, cls, false);
      fallbackSerializer = new DefaultSerializer<>(fury, cls);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, T value) {
      fallbackSerializer.write(fury, buffer, value);
    }

    @Override
    public T read(Fury fury, MemoryBuffer buffer, Class<T> type) {
      return fallbackSerializer.read(fury, buffer, type);
    }
  }
}
