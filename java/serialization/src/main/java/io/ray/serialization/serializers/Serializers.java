package io.ray.serialization.serializers;

import static io.ray.serialization.util.TypeUtils.getArrayComponentInfo;

import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Primitives;
import io.ray.serialization.Fury;
import io.ray.serialization.FuryException;
import io.ray.serialization.resolver.ReferenceResolver;
import io.ray.serialization.util.Descriptor;
import io.ray.serialization.util.MemoryBuffer;
import io.ray.serialization.util.Platform;
import io.ray.serialization.util.Tuple2;
import java.lang.reflect.Array;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Collections;
import java.util.Date;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class Serializers {
  public static final class BooleanSerializer extends Serializer<Boolean> {
    public BooleanSerializer(Fury fury) {
      super(fury, Boolean.class);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, Boolean value) {
      buffer.writeBoolean(value);
    }

    @Override
    public Boolean read(Fury fury, MemoryBuffer buffer, Class<Boolean> type) {
      return buffer.readBoolean();
    }
  }

  public static final class ByteSerializer extends Serializer<Byte> {
    public ByteSerializer(Fury fury) {
      super(fury, Byte.class);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, Byte value) {
      buffer.writeByte(value);
    }

    @Override
    public Byte read(Fury fury, MemoryBuffer buffer, Class<Byte> type) {
      return buffer.readByte();
    }
  }


  public static final class CharSerializer extends Serializer<Character> {
    public CharSerializer(Fury fury) {
      super(fury, Character.class);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, Character value) {
      buffer.writeChar(value);
    }

    @Override
    public Character read(Fury fury, MemoryBuffer buffer, Class<Character> type) {
      return buffer.readChar();
    }
  }

  public static final class ShortSerializer extends Serializer<Short> {
    public ShortSerializer(Fury fury) {
      super(fury, Short.class);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, Short value) {
      buffer.writeShort(value);
    }

    @Override
    public Short read(Fury fury, MemoryBuffer buffer, Class<Short> type) {
      return buffer.readShort();
    }
  }

  public static final class IntSerializer extends Serializer<Integer> {
    public IntSerializer(Fury fury) {
      super(fury, Integer.class);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, Integer value) {
      buffer.writeInt(value);
    }

    @Override
    public Integer read(Fury fury, MemoryBuffer buffer, Class<Integer> type) {
      return buffer.readInt();
    }
  }

  public static final class LongSerializer extends Serializer<Long> {
    public LongSerializer(Fury fury) {
      super(fury, Long.class);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, Long value) {
      buffer.writeLong(value);
    }

    @Override
    public Long read(Fury fury, MemoryBuffer buffer, Class<Long> type) {
      return buffer.readLong();
    }
  }

  public static final class FloatSerializer extends Serializer<Float> {
    public FloatSerializer(Fury fury) {
      super(fury, Float.class);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, Float value) {
      buffer.writeFloat(value);
    }

    @Override
    public Float read(Fury fury, MemoryBuffer buffer, Class<Float> type) {
      return buffer.readFloat();
    }
  }

  public static final class DoubleSerializer extends Serializer<Double> {
    public DoubleSerializer(Fury fury) {
      super(fury, Double.class);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, Double value) {
      buffer.writeDouble(value);
    }

    @Override
    public Double read(Fury fury, MemoryBuffer buffer, Class<Double> type) {
      return buffer.readDouble();
    }
  }

  public static final class LocalDateSerializer extends Serializer<LocalDate> {
    public LocalDateSerializer(Fury fury) {
      super(fury, LocalDate.class);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, LocalDate value) {
      buffer.writeInt(value.getYear());
      buffer.writeByte((byte) value.getMonthValue());
      buffer.writeByte((byte) value.getDayOfMonth());
    }

    @Override
    public LocalDate read(Fury fury, MemoryBuffer buffer, Class<LocalDate> type) {
      return LocalDate.of(buffer.readInt(), buffer.readByte(), buffer.readByte());
    }
  }

  public static final class DateSerializer extends Serializer<Date> {
    public DateSerializer(Fury fury) {
      super(fury, Date.class);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, Date value) {
      buffer.writeLong(value.getTime());
    }

    @Override
    public Date read(Fury fury, MemoryBuffer buffer, Class<Date> type) {
      return new Date(buffer.readLong());
    }
  }

  public static final class TimestampSerializer extends Serializer<Timestamp> {
    public TimestampSerializer(Fury fury) {
      super(fury, Timestamp.class);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, Timestamp value) {
      buffer.writeLong(value.getTime());
    }

    @Override
    public Timestamp read(Fury fury, MemoryBuffer buffer, Class<Timestamp> type) {
      return new Timestamp(buffer.readLong());
    }
  }

  public static final class InstantSerializer extends Serializer<Instant> {
    public InstantSerializer(Fury fury) {
      super(fury, Instant.class);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, Instant value) {
      buffer.writeLong(value.getEpochSecond());
      buffer.writeInt(value.getNano());
    }

    @Override
    public Instant read(Fury fury, MemoryBuffer buffer, Class<Instant> type) {
      return Instant.ofEpochSecond(buffer.readLong(), buffer.readInt());
    }
  }

  public static final class StringBuilderSerializer extends Serializer<StringBuilder> {
    private final StringSerializer stringSerializer;

    public StringBuilderSerializer(Fury fury) {
      super(fury, StringBuilder.class);
      stringSerializer = new StringSerializer(fury);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, StringBuilder value) {
      stringSerializer.writeJavaString(buffer, value.toString());
    }

    @Override
    public StringBuilder read(Fury fury, MemoryBuffer buffer, Class<StringBuilder> type) {
      return new StringBuilder(stringSerializer.readJavaString(buffer));
    }
  }

  public static final class StringBufferSerializer extends Serializer<StringBuffer> {
    private final StringSerializer stringSerializer;

    public StringBufferSerializer(Fury fury) {
      super(fury, StringBuffer.class);
      stringSerializer = new StringSerializer(fury);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, StringBuffer value) {
      stringSerializer.writeJavaString(buffer, value.toString());
    }

    @Override
    public StringBuffer read(Fury fury, MemoryBuffer buffer, Class<StringBuffer> type) {
      return new StringBuffer(stringSerializer.readJavaString(buffer));
    }
  }

  public static final class EnumSerializer extends Serializer<Enum> {
    private final Enum[] enumConstants;

    public EnumSerializer(Fury fury, Class<Enum> cls) {
      super(fury, cls);
      if (cls.isEnum()) {
        enumConstants = cls.getEnumConstants();
      } else {
        Preconditions.checkArgument(Enum.class.isAssignableFrom(cls) && cls != Enum.class);
        @SuppressWarnings("unchecked")
        Class<Enum> enclosingClass = (Class<Enum>) cls.getEnclosingClass();
        Preconditions.checkNotNull(enclosingClass);
        Preconditions.checkArgument(enclosingClass.isEnum());
        enumConstants = enclosingClass.getEnumConstants();
      }
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, Enum value) {
      buffer.writeInt(value.ordinal());
    }

    @Override
    public Enum read(Fury fury, MemoryBuffer buffer, Class<Enum> type) {
      return enumConstants[buffer.readInt()];
    }
  }

  public static final class BigDecimalSerializer extends Serializer<BigDecimal> {
    public BigDecimalSerializer(Fury fury) {
      super(fury, BigDecimal.class);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, BigDecimal value) {
      final byte[] bytes = value.unscaledValue().toByteArray();
      Preconditions.checkArgument(bytes.length <= 16);
      buffer.writeByte((byte) value.scale());
      buffer.writeByte((byte) bytes.length);
      buffer.writeBytes(bytes);
    }

    @Override
    public BigDecimal read(Fury fury, MemoryBuffer buffer, Class<BigDecimal> type) {
      int scale = buffer.readByte();
      int len = buffer.readByte();
      byte[] bytes = buffer.readBytes(len);
      final BigInteger bigInteger = new BigInteger(bytes);
      return new BigDecimal(bigInteger, scale);
    }
  }

  public static final class BigIntegerSerializer extends Serializer<BigInteger> {
    public BigIntegerSerializer(Fury fury) {
      super(fury, BigInteger.class);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, BigInteger value) {
      final byte[] bytes = value.toByteArray();
      Preconditions.checkArgument(bytes.length <= 16);
      buffer.writeByte((byte) bytes.length);
      buffer.writeBytes(bytes);
    }

    @Override
    public BigInteger read(Fury fury, MemoryBuffer buffer, Class<BigInteger> type) {
      int len = buffer.readByte();
      byte[] bytes = buffer.readBytes(len);
      return new BigInteger(bytes);
    }
  }

  public static final class BooleanArraySerializer extends Serializer<boolean[]> {
    public BooleanArraySerializer(Fury fury) {
      super(fury, boolean[].class);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, boolean[] value) {
      writePrimitiveArray(buffer, value, Platform.BOOLEAN_ARRAY_OFFSET, value.length, 1);
    }

    @Override
    public boolean[] read(Fury fury, MemoryBuffer buffer, Class<boolean[]> type) {
      int size = buffer.readInt();
      int numElements = size;
      boolean[] values = new boolean[numElements];
      buffer.copyToUnsafe(buffer.readerIndex(), values, Platform.BOOLEAN_ARRAY_OFFSET, size);
      buffer.readerIndex(buffer.readerIndex() + size);
      return values;
    }
  }

  public static final class ByteArraySerializer extends Serializer<byte[]> {
    public ByteArraySerializer(Fury fury) {
      super(fury, byte[].class);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, byte[] value) {
      fury.writeSerializedObject(buffer, new SerializedObject.ByteArraySerializedObject(value));
    }

    @Override
    public byte[] read(Fury fury, MemoryBuffer buffer, Class<byte[]> type) {
      ByteBuffer buf = fury.readSerializedObject(buffer);
      int remaining = buf.remaining();
      if (buf.hasArray() && remaining == buf.array().length) {
        return buf.array();
      } else {
        byte[] arr = new byte[remaining];
        buf.get(arr);
        return arr;
      }
    }
  }

  public static final class CharArraySerializer extends Serializer<char[]> {
    public CharArraySerializer(Fury fury) {
      super(fury, char[].class);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, char[] value) {
      writePrimitiveArray(buffer, value, Platform.CHAR_ARRAY_OFFSET, value.length, 2);
    }

    @Override
    public char[] read(Fury fury, MemoryBuffer buffer, Class<char[]> type) {
      int size = buffer.readInt();
      int numElements = size / 2;
      char[] values = new char[numElements];
      buffer.copyToUnsafe(buffer.readerIndex(), values, Platform.CHAR_ARRAY_OFFSET, size);
      buffer.readerIndex(buffer.readerIndex() + size);
      return values;
    }
  }

  public static final class ShortArraySerializer extends Serializer<short[]> {
    public ShortArraySerializer(Fury fury) {
      super(fury, short[].class);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, short[] value) {
      writePrimitiveArray(buffer, value, Platform.SHORT_ARRAY_OFFSET, value.length, 2);
    }

    @Override
    public short[] read(Fury fury, MemoryBuffer buffer, Class<short[]> type) {
      int size = buffer.readInt();
      int numElements = size / 2;
      short[] values = new short[numElements];
      buffer.copyToUnsafe(buffer.readerIndex(), values, Platform.SHORT_ARRAY_OFFSET, size);
      buffer.readerIndex(buffer.readerIndex() + size);
      return values;
    }
  }

  public static final class IntArraySerializer extends Serializer<int[]> {
    public IntArraySerializer(Fury fury) {
      super(fury, int[].class);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, int[] value) {
      writePrimitiveArray(buffer, value, Platform.INT_ARRAY_OFFSET, value.length, 4);
    }

    @Override
    public int[] read(Fury fury, MemoryBuffer buffer, Class<int[]> type) {
      int size = buffer.readInt();
      int numElements = size / 4;
      int[] values = new int[numElements];
      buffer.copyToUnsafe(buffer.readerIndex(), values, Platform.INT_ARRAY_OFFSET, size);
      buffer.readerIndex(buffer.readerIndex() + size);
      return values;
    }
  }

  public static final class LongArraySerializer extends Serializer<long[]> {
    public LongArraySerializer(Fury fury) {
      super(fury, long[].class);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, long[] value) {
      writePrimitiveArray(buffer, value, Platform.LONG_ARRAY_OFFSET, value.length, 8);
    }

    @Override
    public long[] read(Fury fury, MemoryBuffer buffer, Class<long[]> type) {
      int size = buffer.readInt();
      int numElements = size / 8;
      long[] values = new long[numElements];
      buffer.copyToUnsafe(buffer.readerIndex(), values, Platform.LONG_ARRAY_OFFSET, size);
      buffer.readerIndex(buffer.readerIndex() + size);
      return values;
    }
  }

  public static final class FloatArraySerializer extends Serializer<float[]> {
    public FloatArraySerializer(Fury fury) {
      super(fury, float[].class);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, float[] value) {
      writePrimitiveArray(buffer, value, Platform.FLOAT_ARRAY_OFFSET, value.length, 4);
    }

    @Override
    public float[] read(Fury fury, MemoryBuffer buffer, Class<float[]> type) {
      int size = buffer.readInt();
      int numElements = size / 4;
      float[] values = new float[numElements];
      buffer.copyToUnsafe(buffer.readerIndex(), values, Platform.FLOAT_ARRAY_OFFSET, size);
      buffer.readerIndex(buffer.readerIndex() + size);
      return values;
    }
  }

  public static final class DoubleArraySerializer extends Serializer<double[]> {
    public DoubleArraySerializer(Fury fury) {
      super(fury, double[].class);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, double[] value) {
      writePrimitiveArray(buffer, value, Platform.DOUBLE_ARRAY_OFFSET, value.length, 8);
    }

    @Override
    public double[] read(Fury fury, MemoryBuffer buffer, Class<double[]> type) {
      int size = buffer.readInt();
      int numElements = size / 8;
      double[] values = new double[numElements];
      buffer.copyToUnsafe(buffer.readerIndex(), values, Platform.DOUBLE_ARRAY_OFFSET, size);
      buffer.readerIndex(buffer.readerIndex() + size);
      return values;
    }
  }

  public static final class StringArraySerializer extends Serializer<String[]> {
    private final StringSerializer stringSerializer;

    public StringArraySerializer(Fury fury) {
      super(fury, String[].class);
      stringSerializer = new StringSerializer(fury);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, String[] value) {
      int len = value.length;
      buffer.writeInt(len);
      for (String elem : value) {
        // TODO reference support
        if (elem != null) {
          buffer.writeByte(Fury.NOT_NULL);
          stringSerializer.writeJavaString(buffer, elem);
        } else {
          buffer.writeByte(Fury.NULL);
        }
      }
    }

    @Override
    public String[] read(Fury fury, MemoryBuffer buffer, Class<String[]> type) {
      int numElements = buffer.readInt();
      String[] value = new String[numElements];
      fury.getReferenceResolver().reference(value);
      for (int i = 0; i < numElements; i++) {
        if (buffer.readByte() == Fury.NOT_NULL) {
          value[i] = stringSerializer.readJavaString(buffer);
        } else {
          value[i] = null;
        }
      }
      return value;
    }
  }

  /** May be multi-dimension array, or multi-dimension primitive array */
  @SuppressWarnings("unchecked")
  public static final class ObjectArraySerializer<T> extends Serializer<T[]> {
    private final Class<T> innerType;
    private final Serializer componentTypeSerializer;
    private final int dimension;
    private final int[] stubDims;

    public ObjectArraySerializer(Fury fury, Class<T[]> cls) {
      super(fury, cls);
      Tuple2<Class<?>, Integer> arrayComponentInfo = getArrayComponentInfo(cls);
      this.dimension = arrayComponentInfo.f1;
      this.innerType = (Class<T>) arrayComponentInfo.f0;
      Class<?> componentType = cls.getComponentType();
      if (Modifier.isFinal(componentType.getModifiers())) {
        this.componentTypeSerializer = fury.getClassResolver().getSerializer(componentType);
      } else {
        this.componentTypeSerializer = null;
      }
      this.stubDims = new int[dimension];
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, T[] arr) {
      int len = arr.length;
      buffer.writeInt(len);
      final Serializer componentTypeSerializer = this.componentTypeSerializer;
      if (componentTypeSerializer != null) {
        ReferenceResolver referenceResolver = fury.getReferenceResolver();
        for (T t : arr) {
          if (!referenceResolver.writeReferenceOrNull(buffer, t)) {
            componentTypeSerializer.write(fury, buffer, t);
          }
        }
      } else {
        for (T t : arr) {
          fury.serializeReferencableToJava(buffer, t);
        }
      }
    }

    @Override
    public T[] read(Fury fury, MemoryBuffer buffer, Class<T[]> type) {
      int numElements = buffer.readInt();
      Object[] value = newArray(numElements);
      ReferenceResolver referenceResolver = fury.getReferenceResolver();
      referenceResolver.reference(value);
      final Serializer componentTypeSerializer = this.componentTypeSerializer;
      final Class<T> innerType = this.innerType;
      if (componentTypeSerializer != null) {
        boolean referenceTracking = fury.isReferenceTracking();
        for (int i = 0; i < numElements; i++) {
          Object elem = null;
          // It's not a reference, we need read field data.
          if (referenceResolver.readReferenceOrNull(buffer) == Fury.NOT_NULL) {
            int nextReadRefId = referenceResolver.preserveReferenceId();
            elem = componentTypeSerializer.read(fury, buffer, innerType);
            if (referenceTracking) {
              referenceResolver.setReadObject(nextReadRefId, elem);
            }
          } else {
            if (referenceTracking) {
              elem = referenceResolver.getReadObject();
            }
          }
          value[i] = elem;
        }
      } else {
        for (int i = 0; i < numElements; i++) {
          value[i] = fury.deserializeReferencableFromJava(buffer);
        }
      }
      return (T[]) value;
    }

    private Object[] newArray(int numElements) {
      Object[] value;
      if ((Class) cls == Object[].class) {
        value = new Object[numElements];
      } else {
        stubDims[0] = numElements;
        value = (Object[]) Array.newInstance(innerType, stubDims);
      }
      return value;
    }
  }

  // ------------------------------ collections serializers ------------------------------ //
  // For cross-language serialization, if the data is passed from python, the data will be
  // deserialized by `MapSerializers` and `CollectionSerializers`.
  // But if the data is serialized by following collections serializers, we need to ensure the real
  // type of `crossLanguageRead` is the same as the type when serialize.
  public static final class CollectionsEmptyListSerializer extends Serializer<List<?>> {

    public CollectionsEmptyListSerializer(Fury fury, Class<List<?>> cls) {
      super(fury, cls);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, List<?> value) {}

    @Override
    public List<?> read(Fury fury, MemoryBuffer buffer, Class<List<?>> type) {
      return Collections.EMPTY_LIST;
    }
  }

  public static final class CollectionsEmptySetSerializer extends Serializer<Set<?>> {

    public CollectionsEmptySetSerializer(Fury fury, Class<Set<?>> cls) {
      super(fury, cls);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, Set<?> value) {}

    @Override
    public Set<?> read(Fury fury, MemoryBuffer buffer, Class<Set<?>> type) {
      return Collections.EMPTY_SET;
    }
  }

  public static final class CollectionsEmptyMapSerializer extends Serializer<Map<?, ?>> {

    public CollectionsEmptyMapSerializer(Fury fury, Class<Map<?, ?>> cls) {
      super(fury, cls);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, Map<?, ?> value) {}

    @Override
    public Map<?, ?> read(Fury fury, MemoryBuffer buffer, Class<Map<?, ?>> type) {
      return Collections.EMPTY_MAP;
    }
  }

  public static final class CollectionsSingletonListSerializer extends Serializer<List<?>> {

    public CollectionsSingletonListSerializer(Fury fury, Class<List<?>> cls) {
      super(fury, cls);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, List<?> value) {
      fury.serializeReferencableToJava(buffer, value.get(0));
    }

    @Override
    public List<?> read(Fury fury, MemoryBuffer buffer, Class<List<?>> type) {
      return Collections.singletonList(fury.deserializeReferencableFromJava(buffer));
    }
  }

  public static final class CollectionsSingletonSetSerializer extends Serializer<Set<?>> {

    public CollectionsSingletonSetSerializer(Fury fury, Class<Set<?>> cls) {
      super(fury, cls);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, Set<?> value) {
      fury.serializeReferencableToJava(buffer, value.iterator().next());
    }

    @Override
    public Set<?> read(Fury fury, MemoryBuffer buffer, Class<Set<?>> type) {
      return Collections.singleton(fury.deserializeReferencableFromJava(buffer));
    }
  }

  public static final class CollectionsSingletonMapSerializer extends Serializer<Map<?, ?>> {

    public CollectionsSingletonMapSerializer(Fury fury, Class<Map<?, ?>> cls) {
      super(fury, cls);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, Map<?, ?> value) {
      Entry entry = value.entrySet().iterator().next();
      fury.serializeReferencableToJava(buffer, entry.getKey());
      fury.serializeReferencableToJava(buffer, entry.getValue());
    }

    @Override
    public Map<?, ?> read(Fury fury, MemoryBuffer buffer, Class<Map<?, ?>> type) {
      Object key = fury.deserializeReferencableFromJava(buffer);
      Object value = fury.deserializeReferencableFromJava(buffer);
      return Collections.singletonMap(key, value);
    }
  }

  public static final class ClassSerializer extends Serializer<Class> {
    private static final byte USE_CLASS_ID = 0;
    private static final byte USE_CLASSNAME = 1;
    private static final byte PRIMITIVE_FLAG = 2;
    private final IdentityHashMap<Class<?>, Byte> primitivesMap = new IdentityHashMap<>();
    private final Class<?>[] id2PrimitiveClasses = new Class[Primitives.allPrimitiveTypes().size()];

    public ClassSerializer(Fury fury) {
      super(fury, Class.class);
      byte count = 0;
      for (Class<?> primitiveType : Primitives.allPrimitiveTypes()) {
        primitivesMap.put(primitiveType, count);
        id2PrimitiveClasses[count] = primitiveType;
        count++;
      }
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, Class value) {
      Short classId = fury.getClassResolver().getRegisteredClassId(value);
      if (classId != null) {
        buffer.writeByte(USE_CLASS_ID);
        buffer.writeShort(classId);
      } else {
        if (value.isPrimitive()) {
          buffer.writeByte(PRIMITIVE_FLAG);
          buffer.writeByte(primitivesMap.get(value));
        } else {
          buffer.writeByte(USE_CLASSNAME);
          fury.getClassResolver().writeClassNameBytes(buffer, value);
        }
      }
    }

    @Override
    public Class read(Fury fury, MemoryBuffer buffer, Class<Class> type) {
      byte tag = buffer.readByte();
      if (tag == USE_CLASS_ID) {
        return fury.getClassResolver().getRegisteredClass(buffer.readShort());
      } else {
        if (tag == PRIMITIVE_FLAG) {
          return id2PrimitiveClasses[buffer.readByte()];
        } else {
          return fury.getClassResolver().readClassByClassNameBytes(buffer);
        }
      }
    }
  }

  // ########################## out of band serialization ##########################

  /**
   * Note that this serializer only serialize data, but not the byte buffer meta. Since ByteBuffer
   * doesn't implement {@link java.io.Serializable}, it's ok to only serialize data. Also Note that
   * a direct buffer may be returned if the serialized buffer is a heap buffer.
   */
  public static final class ByteBufferSerializer extends Serializer<ByteBuffer> {

    public ByteBufferSerializer(Fury fury, Class<ByteBuffer> cls) {
      super(fury, cls);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, ByteBuffer value) {
      fury.writeSerializedObject(buffer, new SerializedObject.ByteBufferSerializedObject(value));
    }

    @Override
    public ByteBuffer read(Fury fury, MemoryBuffer buffer, Class<ByteBuffer> type) {
      return fury.readSerializedObject(buffer);
    }
  }

  // ########################## utils ##########################
  static void writePrimitiveArray(
      MemoryBuffer buffer, Object arr, int offset, int numElements, int elemSize) {
    int size = Math.multiplyExact(numElements, elemSize);
    buffer.writeInt(size);
    buffer.grow(size);
    buffer.copyFromUnsafe(buffer.writerIndex(), arr, offset, size);
    buffer.writerIndex(buffer.writerIndex() + size);
  }

  public static void checkClassVersion(Fury fury, int readHash, int classVersionHash) {
    if (readHash != classVersionHash) {
      throw new FuryException(
        String.format(
          "Read class %s version %s is not consistent with %s",
          fury.getClassResolver().getCurrentReadClass(), readHash, classVersionHash));
    }
  }

  @SuppressWarnings("UnstableApiUsage")
  public static int computeVersionHash(Class<?> clz) {
    int hash =
      Descriptor.getFields(clz).stream()
        .map(
          f ->
            Objects.hash(
              f.getName(), f.getType().getName(), f.getDeclaringClass().getName()))
        .collect(Collectors.toList())
        .hashCode();
    return Hashing.murmur3_32().hashInt(hash).asInt();
  }
}
