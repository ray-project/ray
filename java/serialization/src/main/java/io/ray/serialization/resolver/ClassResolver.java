package io.ray.serialization.resolver;

import static io.ray.serialization.serializers.JavaSerializers.JdkProxySerializer;
import static io.ray.serialization.serializers.JavaSerializers.isDynamicGeneratedCLass;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBiMap;
import io.ray.serialization.Fury;
import io.ray.serialization.serializers.CodegenSerializer;
import io.ray.serialization.serializers.CollectionSerializers;
import io.ray.serialization.serializers.DefaultSerializer;
import io.ray.serialization.serializers.ExternalizableSerializer;
import io.ray.serialization.serializers.JavaSerializers.JavaSerializer;
import io.ray.serialization.serializers.JavaSerializers.LambdaSerializer;
import io.ray.serialization.serializers.MapSerializers;
import io.ray.serialization.serializers.Serializer;
import io.ray.serialization.serializers.SerializerFactory;
import io.ray.serialization.serializers.Serializers;
import io.ray.serialization.serializers.StringSerializer;
import io.ray.serialization.serializers.UnmodifiableCollectionSerializer;
import io.ray.serialization.util.LoggerFactory;
import io.ray.serialization.util.MemoryBuffer;
import io.ray.serialization.util.Platform;
import java.io.Externalizable;
import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.slf4j.Logger;

@SuppressWarnings("UnstableApiUsage")
public class ClassResolver {
  private static final Logger LOG = LoggerFactory.getLogger(ClassResolver.class);

  private static final byte USE_CLASSNAME = 0;
  private static final byte USE_CLASS_ID = 1;

  private static final Short LONG_CLASS_ID = 0;
  private static final Short INTEGER_CLASS_ID = 1;
  private static final Short DOUBLE_CLASS_ID = 2;
  private static final Short LAMBDA_STUB_ID = 3;
  private static final Short JDK_PROXY_STUB_ID = 4;

  private Short registeredClassIdCounter = 0;
  private final IdentityHashMap<Class<?>, Short> registeredClassIdMap = new IdentityHashMap<>(1000);
  private ClassInfo[] registeredId2ClassInfo = new ClassInfo[] {};
  // avoid potential recursive call for seq codec generation.
  // ex. A->field1: B, B.field1: A
  private Set<Class<?>> getClassCtx = new HashSet<>();

  private int initialCapacity = 64;
  // use a lower load factor to minimize hash collision
  private float loadFactor = 0.25f;
  // IdentityHashMap has better performance, avoid polymorphic equals/hashCode
  private final IdentityHashMap<Class<?>, ClassInfo> classInfos =
      new IdentityHashMap<>(initialCapacity);
  private ClassInfo classInfoCache = new ClassInfo(null, null, null, null);
  private final IdentityHashMap<Class<?>, Bytes> class2ClassNameBytes = new IdentityHashMap<>();
  private final Map<Bytes, Class<?>> classNameBytes2Class =
      new HashMap<>(initialCapacity, loadFactor);
  private final Map<Bytes, String> classNameBytes2Classname =
      new HashMap<>(initialCapacity, loadFactor);
  private final Map<String, Class<?>> className2Class = new HashMap<>(initialCapacity, loadFactor);
  private final Map<Class<?>, Short> classToTypeIdXLangMap =
      new HashMap<>(initialCapacity, loadFactor);
  private final Map<Short, Class<?>> typeIdToClassXLangMap =
      new HashMap<>(initialCapacity, loadFactor);
  private final Map<String, Class<?>> typeTagToClassXLangMap =
      new HashMap<>(initialCapacity, loadFactor);
  private final Fury fury;
  private Class<?> currentReadClass;
  private HashBiMap<Bytes, Short> dynamicClassIdMap;
  private short dynamicWriteClassId;
  private short dynamicReadClassId;
  private SerializerFactory serializerFactory;

  public ClassResolver(Fury fury) {
    this.fury = fury;
    dynamicClassIdMap = HashBiMap.create();
    dynamicWriteClassId = 0;
    dynamicReadClassId = 0;
  }

  public void initialize() {
    register(Long.class, LONG_CLASS_ID);
    register(Integer.class, INTEGER_CLASS_ID);
    register(Double.class, DOUBLE_CLASS_ID);
    register(LambdaSerializer.ReplaceStub.class, LAMBDA_STUB_ID);
    register(JdkProxySerializer.ReplaceStub.class, JDK_PROXY_STUB_ID);
    addDefaultSerializers();
    registerDefaultClasses();
  }

  private void addDefaultSerializers() {
    // primitive types will be boxed.
    addDefaultSerializer(Boolean.class, Serializers.BooleanSerializer.class);
    addDefaultSerializer(Byte.class, Serializers.ByteSerializer.class);
    addDefaultSerializer(Character.class, Serializers.CharSerializer.class);
    addDefaultSerializer(Short.class, Serializers.ShortSerializer.class);
    addDefaultSerializer(Integer.class, Serializers.IntSerializer.class);
    addDefaultSerializer(Long.class, Serializers.LongSerializer.class);
    addDefaultSerializer(Float.class, Serializers.FloatSerializer.class);
    addDefaultSerializer(Double.class, Serializers.DoubleSerializer.class);
    addDefaultSerializer(String.class, StringSerializer.class);
    addDefaultSerializer(StringBuilder.class, Serializers.StringBuilderSerializer.class);
    addDefaultSerializer(StringBuffer.class, Serializers.StringBufferSerializer.class);
    addDefaultSerializer(BigInteger.class, Serializers.BigIntegerSerializer.class);
    addDefaultSerializer(BigDecimal.class, Serializers.BigDecimalSerializer.class);
    addDefaultSerializer(LocalDate.class, Serializers.LocalDateSerializer.class);
    addDefaultSerializer(Date.class, Serializers.DateSerializer.class);
    addDefaultSerializer(Timestamp.class, Serializers.TimestampSerializer.class);
    addDefaultSerializer(Instant.class, Serializers.InstantSerializer.class);
    addDefaultSerializer(byte[].class, Serializers.ByteArraySerializer.class);
    addDefaultSerializer(char[].class, Serializers.CharArraySerializer.class);
    addDefaultSerializer(short[].class, Serializers.ShortArraySerializer.class);
    addDefaultSerializer(int[].class, Serializers.IntArraySerializer.class);
    addDefaultSerializer(long[].class, Serializers.LongArraySerializer.class);
    addDefaultSerializer(float[].class, Serializers.FloatArraySerializer.class);
    addDefaultSerializer(double[].class, Serializers.DoubleArraySerializer.class);
    addDefaultSerializer(boolean[].class, Serializers.BooleanArraySerializer.class);
    addDefaultSerializer(String[].class, Serializers.StringArraySerializer.class);
    addDefaultSerializer(
        Object[].class, new Serializers.ObjectArraySerializer<>(fury, Object[].class));
    addDefaultSerializer(ArrayList.class, CollectionSerializers.ArrayListSerializer.class);
    addDefaultSerializer(LinkedList.class, CollectionSerializers.CollectionSerializer.class);
    addDefaultSerializer(HashSet.class, CollectionSerializers.HashSetSerializer.class);
    addDefaultSerializer(LinkedHashSet.class, CollectionSerializers.HashSetSerializer.class);
    addDefaultSerializer(
        TreeSet.class, new CollectionSerializers.SortedSetSerializer<>(fury, TreeSet.class));
    addDefaultSerializer(HashMap.class, MapSerializers.HashMapSerializer.class);
    addDefaultSerializer(LinkedHashMap.class, MapSerializers.HashMapSerializer.class);
    addDefaultSerializer(
        TreeMap.class, new MapSerializers.SortedMapSerializer<>(fury, TreeMap.class));

    addDefaultSerializer(
        Collections.EMPTY_LIST.getClass(), Serializers.CollectionsEmptyListSerializer.class);
    addDefaultSerializer(
        Collections.EMPTY_SET.getClass(), Serializers.CollectionsEmptySetSerializer.class);
    addDefaultSerializer(
        Collections.EMPTY_MAP.getClass(), Serializers.CollectionsEmptyMapSerializer.class);
    addDefaultSerializer(
        Collections.singletonList(null).getClass(),
        Serializers.CollectionsSingletonListSerializer.class);
    addDefaultSerializer(
        Collections.singleton(null).getClass(),
        Serializers.CollectionsSingletonSetSerializer.class);
    addDefaultSerializer(
        Collections.singletonMap(null, null).getClass(),
        Serializers.CollectionsSingletonMapSerializer.class);

    addDefaultSerializer(LambdaSerializer.ReplaceStub.class, LambdaSerializer.class);
    addDefaultSerializer(JdkProxySerializer.ReplaceStub.class, JdkProxySerializer.class);
    addDefaultSerializer(Class.class, Serializers.ClassSerializer.class);
    UnmodifiableCollectionSerializer.registerSerializers(fury);
  }

  private void addDefaultSerializer(Class type, Class<? extends Serializer> serializerClass) {
    addDefaultSerializer(type, Serializer.newSerializer(fury, type, serializerClass));
  }

  private void addDefaultSerializer(Class type, Serializer serializer) {
    registerSerializer(type, serializer);
    register(type);
  }

  private void registerDefaultClasses() {
    register(Class.class);
    register(SerializedLambda.class);
    register(ConcurrentHashMap.class);
    register(ArrayBlockingQueue.class);
    register(LinkedBlockingQueue.class);
    register(AtomicBoolean.class);
    register(AtomicInteger.class);
    register(AtomicLong.class);
    register(AtomicReference.class);
    register(java.util.Comparator.naturalOrder().getClass());
    register(java.util.Comparator.reverseOrder().getClass());
    register(Collections.unmodifiableCollection(new ArrayList<>()).getClass());
    register(Collections.unmodifiableList(new ArrayList<>()).getClass());
    register(Collections.unmodifiableList(new LinkedList<>()).getClass());
    register(Collections.unmodifiableMap(new HashMap<>()).getClass());
    register(Collections.unmodifiableSet(new HashSet<>()).getClass());
    register(Collections.unmodifiableSortedSet(new TreeSet<>()).getClass());
    register(Collections.unmodifiableSortedMap(new TreeMap<>()).getClass());
  }

  /** register class */
  public void register(Class<?> cls) {
    if (!registeredClassIdMap.containsKey(cls)) {
      while (registeredClassIdMap.containsValue(registeredClassIdCounter)) {
        registeredClassIdCounter++;
      }
      register(cls, registeredClassIdCounter);
      registeredClassIdCounter++;
    }
  }

  /** register class with given id */
  public void register(Class<?> cls, Short id) {
    Preconditions.checkArgument(id >= 0);
    if (!registeredClassIdMap.containsKey(cls)) {
      Preconditions.checkArgument(!registeredClassIdMap.containsValue(id));
      registeredClassIdMap.put(cls, id);
      if (registeredId2ClassInfo.length <= id) {
        ClassInfo[] tmp = new ClassInfo[(id + 1) * 2];
        System.arraycopy(registeredId2ClassInfo, 0, tmp, 0, registeredId2ClassInfo.length);
        registeredId2ClassInfo = tmp;
      }
      ClassInfo classInfo = classInfos.get(cls);
      Serializer serializer = null;
      if (classInfo != null) {
        classInfo.classId = id;
        serializer = classInfo.serializer;
      }
      // serializer will be set lazily in `addSerializer` method if it's null.
      registeredId2ClassInfo[id] = new ClassInfo(cls, createClassNameBytes(cls), serializer, id);
    }
  }

  public Short getRegisteredClassId(Class<?> cls) {
    return registeredClassIdMap.get(cls);
  }

  public Class<?> getRegisteredClass(short id) {
    if (id < registeredId2ClassInfo.length) {
      ClassInfo classInfo = registeredId2ClassInfo[id];
      if (classInfo != null) {
        return classInfo.cls;
      }
    }
    return null;
  }

  public List<Class<?>> getRegisteredClasses() {
    return Arrays.stream(registeredId2ClassInfo)
        .filter(Objects::nonNull)
        .map(info -> info.cls)
        .collect(Collectors.toList());
  }

  /**
   * @param type class needed to be serialized/deserialized
   * @param serializerClass serializer class can be created with {@link Serializer#newSerializer)}
   * @param <T> type of class
   */
  public <T> void registerSerializer(
      Class<T> type, Class<? extends Serializer<T>> serializerClass) {
    registerSerializer(type, Serializer.newSerializer(fury, type, serializerClass));
  }

  /**
   * If a serializer exists before, it will be replaced by new serializer.
   *
   * @param type class needed to be serialized/deserialized
   * @param serializer serializer for object of {@code type}
   */
  public void registerSerializer(Class<?> type, Serializer<?> serializer) {
    addSerializer(type, serializer);
  }

  public void setSerializerFactory(SerializerFactory serializerFactory) {
    this.serializerFactory = serializerFactory;
  }

  public SerializerFactory getSerializerFactory() {
    return serializerFactory;
  }

  private void addSerializer(Class<?> type, Serializer<?> serializer) {
    Short classId = registeredClassIdMap.get(type);
    // set serializer for class if it's registered by now.
    if (classId != null) {
      registeredId2ClassInfo[classId].serializer = serializer;
    }

    // class id will be set in `register` method if it's null.
    ClassInfo classInfo = new ClassInfo(type, createClassNameBytes(type), serializer, classId);
    classInfos.put(type, classInfo);
  }

  /** Get or create serializer for <code>cls</code> */
  public Serializer<?> getSerializer(Class<?> cls) {
    Preconditions.checkNotNull(cls);
    return getOrUpdateClassInfo(cls).serializer;
  }

  @SuppressWarnings("unchecked")
  public <T> Serializer<T> getTypedSerializer(Class<T> cls) {
    return (Serializer<T>) getSerializer(cls);
  }

  public Class<? extends Serializer> getSerializerClass(Class<?> cls) {
    ClassInfo classInfo = classInfos.get(cls);
    if (classInfo != null) {
      return classInfo.serializer.getClass();
    } else {
      if (cls.isEnum()) {
        return Serializers.EnumSerializer.class;
      } else if (Enum.class.isAssignableFrom(cls) && cls != Enum.class) {
        // handles an enum value that is an inner class. Eg: enum A {b{}};
        return Serializers.EnumSerializer.class;
      } else if (cls.isArray()) {
        Preconditions.checkArgument(!cls.getComponentType().isPrimitive());
        return Serializers.ObjectArraySerializer.class;
      } else if (LambdaSerializer.isLambda(cls)) {
        return LambdaSerializer.class;
      } else if (JdkProxySerializer.isJdkProxy(cls)) {
        return JdkProxySerializer.class;
      } else if (JavaSerializer.requireJavaSerialization(cls)) {
        return JavaSerializer.getJavaSerializer(cls);
      } else if (Externalizable.class.isAssignableFrom(cls)) {
        return ExternalizableSerializer.class;
      } else if (ByteBuffer.class.isAssignableFrom(cls)) {
        return Serializers.ByteBufferSerializer.class;
      }
      if (fury.checkJdkClassSerializable()) {
        if (cls.getName().startsWith("java") && !(Serializable.class.isAssignableFrom(cls))) {
          throw new UnsupportedOperationException(
              String.format("Class %s doesn't support serialization.", cls));
        }
      }
      if (Collection.class.isAssignableFrom(cls)) {
        // Serializer of common collection such as ArrayList/LinkedList should be registered already
        return CollectionSerializers.CollectionDefaultJavaSerializer.class;
      } else if (Map.class.isAssignableFrom(cls)) {
        // Serializer of common map such as HashMap/LinkedHashMap should be registered already.
        return MapSerializers.MapDefaultJavaSerializer.class;
      }
      LOG.warn("Class {} isn't supported for cross-language serialization.", cls);
      if (CodegenSerializer.support(fury, cls) && fury.isCodeGenEnabled()) {
        if (getClassCtx.size() > 0) {
          // avoid potential recursive call for seq codec generation.
          return CodegenSerializer.LazyInitBeanSerializer.class;
        } else {
          getClassCtx.add(cls);
          Class<? extends Serializer<?>> serializerClass =
              CodegenSerializer.loadCodegenSerializer(fury, cls);
          getClassCtx.remove(cls);
          return serializerClass;
        }
      } else {
        return DefaultSerializer.class;
      }
    }
  }

  private ClassInfo getOrUpdateClassInfo(Class<?> cls) {
    ClassInfo classInfo = classInfoCache;
    if (classInfo.cls != cls) {
      classInfo = classInfos.get(cls);
      if (classInfo == null || classInfo.serializer == null) {
        addSerializer(cls, createSerializer(cls));
        classInfo = classInfos.get(cls);
      }
      classInfoCache = classInfo;
    }
    return classInfo;
  }

  private ClassInfo getOrUpdateClassInfo(short classId) {
    ClassInfo classInfo = registeredId2ClassInfo[classId];
    if (classInfo.serializer == null) {
      addSerializer(classInfo.cls, createSerializer(classInfo.cls));
      classInfo = classInfos.get(classInfo.cls);
      classInfoCache = classInfo;
    }
    return classInfo;
  }

  private Serializer createSerializer(Class<?> cls) {
    if (!registeredClassIdMap.containsKey(cls)) {
      LOG.info(
          "{} is not registered, serialize object of this class will " + "write class name.", cls);
    }
    if (serializerFactory != null) {
      Serializer serializer = serializerFactory.createSerializer(fury, cls);
      if (serializer != null) {
        return serializer;
      }
    }
    Class<? extends Serializer> serializerClass = getSerializerClass(cls);
    return Serializer.newSerializer(fury, cls, serializerClass);
  }

  /** Write class info to <code>buffer</code> */
  public void writeClass(MemoryBuffer buffer, Class<?> cls) {
    // fast path for common type
    if (cls == Long.class) {
      buffer.writeByte(USE_CLASS_ID);
      buffer.writeShort(LONG_CLASS_ID);
    } else if (cls == Integer.class) {
      buffer.writeByte(USE_CLASS_ID);
      buffer.writeShort(INTEGER_CLASS_ID);
    } else if (cls == Double.class) {
      buffer.writeByte(USE_CLASS_ID);
      buffer.writeShort(DOUBLE_CLASS_ID);
    } else {
      ClassInfo classInfo = getOrUpdateClassInfo(cls);
      if (classInfo.classId == null) {
        // use classname
        buffer.writeByte(USE_CLASSNAME);
        Bytes classNameBytes = classInfo.classNameBytes;
        writeClassNameBytes(buffer, classNameBytes);
      } else {
        // use classId
        buffer.writeByte(USE_CLASS_ID);
        buffer.writeShort(classInfo.classId);
      }
    }
  }

  /** Read class info from <code>buffer</code> as a Class */
  public Class<?> readClass(MemoryBuffer buffer) {
    if (buffer.readByte() == USE_CLASSNAME) {
      final Class<?> cls = readClassByClassNameBytes(buffer);
      currentReadClass = cls;
      return cls;
    } else {
      // use classId
      short classId = buffer.readShort();
      ClassInfo classInfo = getOrUpdateClassInfo(classId);
      final Class<?> cls = classInfo.cls;
      currentReadClass = cls;
      return cls;
    }
  }

  public Class<?> getCurrentReadClass() {
    return currentReadClass;
  }

  public void writeClassNameBytes(MemoryBuffer buffer, Class<?> cls) {
    Bytes bytes = class2ClassNameBytes.get(cls);
    if (bytes == null) {
      bytes = createClassNameBytes(cls);
      class2ClassNameBytes.put(cls, bytes);
    }
    writeClassNameBytes(buffer, bytes);
  }

  private void writeClassNameBytes(MemoryBuffer buffer, Bytes byteString) {
    Short classId = dynamicClassIdMap.get(byteString);
    if (classId == null) {
      classId = dynamicWriteClassId++;
      dynamicClassIdMap.put(byteString, classId);
      buffer.writeByte(USE_CLASSNAME);
      buffer.writeInt(byteString.hashCode);
      buffer.writeShort((short) byteString.bytes.length);
      buffer.writeBytes(byteString.bytes);
    } else {
      buffer.writeByte(USE_CLASS_ID);
      buffer.writeShort(classId);
    }
  }

  private Bytes createClassNameBytes(Class<?> cls) {
    return createClassNameBytes(cls.getName());
  }

  private Bytes createClassNameBytes(String clsName) {
    byte[] classNameBytes = clsName.getBytes(StandardCharsets.UTF_8);
    Preconditions.checkArgument(classNameBytes.length <= Short.MAX_VALUE);
    return new Bytes(classNameBytes, clsName.hashCode());
  }

  public Class<?> readClassByClassNameBytes(MemoryBuffer buffer) {
    Bytes byteString = readClassNameBytes(buffer);
    Class<?> cls = classNameBytes2Class.get(byteString);
    if (cls == null) {
      String className = new String(byteString.bytes, StandardCharsets.UTF_8);
      try {
        cls = Class.forName(className, false, fury.getClassLoader());
        classNameBytes2Class.put(byteString, cls);
      } catch (ClassNotFoundException e) {
        String msg = String.format("class [%s] not found", className);
        throw new IllegalStateException(msg, e);
      }
    }
    return cls;
  }

  public Class<?> readClassByClassName(String className) {
    Class<?> cls = className2Class.get(className);
    if (cls == null) {
      try {
        cls = Class.forName(className, false, fury.getClassLoader());
        className2Class.put(className, cls);
        return cls;
      } catch (ClassNotFoundException e) {
        String msg = String.format("class [%s] not found", className);
        throw new IllegalStateException(msg, e);
      }
    }
    return cls;
  }

  public String readClassName(MemoryBuffer buffer) {
    Bytes byteString = readClassNameBytes(buffer);
    return classNameBytes2Classname.computeIfAbsent(
        byteString, s -> new String(s.bytes, StandardCharsets.UTF_8));
  }

  private Bytes readClassNameBytes(MemoryBuffer buffer) {
    Bytes byteString;
    if (buffer.readByte() == USE_CLASSNAME) {
      int hashCode = buffer.readInt();
      int classNameBytesLength = buffer.readShort();
      byte[] classNameBytes = buffer.readBytes(classNameBytesLength);
      byteString = new Bytes(classNameBytes, hashCode);
      dynamicClassIdMap.inverse().put(dynamicReadClassId++, byteString);
    } else {
      byteString = dynamicClassIdMap.inverse().get(buffer.readShort());
    }
    return byteString;
  }

  public void reset() {
    if (dynamicClassIdMap.size() > 0) {
      dynamicClassIdMap.clear();
      dynamicWriteClassId = 0;
      dynamicReadClassId = 0;
    }
  }

  public void resetRead() {
    if (dynamicReadClassId != 0) {
      dynamicReadClassId = 0;
      dynamicClassIdMap.clear();
    }
  }

  public void resetWrite() {
    if (dynamicWriteClassId != 0) {
      dynamicWriteClassId = 0;
      dynamicClassIdMap.clear();
    }
  }

  public Class<?> getClassByTypeId(short typeId) {
    return typeIdToClassXLangMap.get(typeId);
  }

  public Class<?> readClassByTypeTag(MemoryBuffer buffer) {
    String tag = readClassName(buffer);
    return typeTagToClassXLangMap.get(tag);
  }

  private static class Bytes {
    private final byte[] bytes;
    private final int hashCode;

    public Bytes(byte[] bytes, int hashCode) {
      Preconditions.checkNotNull(bytes);
      this.bytes = bytes;
      this.hashCode = hashCode;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Bytes that = (Bytes) o;
      if (hashCode == that.hashCode) {
        if (bytes == that.bytes) {
          return true;
        }
        // `Platform.arrayEquals` have better performance than `Arrays.equals` when bytes length >=
        // 8
        if (bytes.length > 8 && bytes.length == that.bytes.length) {
          return Platform.arrayEquals(
              bytes,
              Platform.BYTE_ARRAY_OFFSET,
              that.bytes,
              Platform.BYTE_ARRAY_OFFSET,
              bytes.length);
        } else {
          return Arrays.equals(bytes, that.bytes);
        }
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public String toString() {
      return Arrays.toString(bytes);
    }
  }

  private static class ClassInfo {
    final Class<?> cls;
    final Bytes classNameBytes;
    final boolean isDynamicGeneratedClass;
    Serializer<?> serializer;
    Short classId;

    private ClassInfo(Class<?> cls, Bytes classNameBytes, Serializer<?> serializer, Short classId) {
      this.cls = cls;
      this.serializer = serializer;
      this.classNameBytes = classNameBytes;
      this.classId = classId;
      if (cls != null) {
        this.isDynamicGeneratedClass = isDynamicGeneratedCLass(cls);
        if (LambdaSerializer.isLambda(cls)) {
          this.classId = LAMBDA_STUB_ID;
        }
        if (JdkProxySerializer.isJdkProxy(cls)) {
          this.classId = JDK_PROXY_STUB_ID;
        }
      } else {
        this.isDynamicGeneratedClass = false;
      }
    }

    @Override
    public String toString() {
      return "ClassInfo{"
          + "cls="
          + cls
          + ", classNameBytes="
          + classNameBytes
          + ", isDynamicGeneratedClass="
          + isDynamicGeneratedClass
          + ", serializer="
          + serializer
          + ", classId="
          + classId
          + '}';
    }
  }
}
