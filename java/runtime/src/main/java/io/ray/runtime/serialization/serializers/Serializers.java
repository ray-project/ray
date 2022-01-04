package io.ray.runtime.serialization.serializers;

import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Primitives;
import io.ray.runtime.io.MemoryBuffer;
import io.ray.runtime.io.Platform;
import io.ray.runtime.serialization.RaySerde;
import io.ray.runtime.serialization.SerdeException;
import io.ray.runtime.serialization.resolver.ReferenceResolver;
import io.ray.runtime.serialization.util.Descriptor;
import io.ray.runtime.serialization.util.Tuple2;
import io.ray.runtime.serialization.util.TypeUtils;
import java.lang.reflect.Array;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Date;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.stream.Collectors;

public class Serializers {
  public static final class BooleanSerializer extends Serializer<Boolean> {
    public BooleanSerializer(RaySerde raySerDe) {
      super(raySerDe, Boolean.class);
    }

    @Override
    public void write(MemoryBuffer buffer, Boolean value) {
      buffer.writeBoolean(value);
    }

    @Override
    public Boolean read(MemoryBuffer buffer) {
      return buffer.readBoolean();
    }
  }

  public static final class ByteSerializer extends Serializer<Byte> {
    public ByteSerializer(RaySerde raySerDe) {
      super(raySerDe, Byte.class);
    }

    @Override
    public void write(MemoryBuffer buffer, Byte value) {
      buffer.writeByte(value);
    }

    @Override
    public Byte read(MemoryBuffer buffer) {
      return buffer.readByte();
    }
  }

  public static final class CharSerializer extends Serializer<Character> {
    public CharSerializer(RaySerde raySerDe) {
      super(raySerDe, Character.class);
    }

    @Override
    public void write(MemoryBuffer buffer, Character value) {
      buffer.writeChar(value);
    }

    @Override
    public Character read(MemoryBuffer buffer) {
      return buffer.readChar();
    }
  }

  public static final class ShortSerializer extends Serializer<Short> {
    public ShortSerializer(RaySerde raySerDe) {
      super(raySerDe, Short.class);
    }

    @Override
    public void write(MemoryBuffer buffer, Short value) {
      buffer.writeShort(value);
    }

    @Override
    public Short read(MemoryBuffer buffer) {
      return buffer.readShort();
    }
  }

  public static final class IntSerializer extends Serializer<Integer> {
    public IntSerializer(RaySerde raySerDe) {
      super(raySerDe, Integer.class);
    }

    @Override
    public void write(MemoryBuffer buffer, Integer value) {
      buffer.writeInt(value);
    }

    @Override
    public Integer read(MemoryBuffer buffer) {
      return buffer.readInt();
    }
  }

  public static final class LongSerializer extends Serializer<Long> {
    public LongSerializer(RaySerde raySerDe) {
      super(raySerDe, Long.class);
    }

    @Override
    public void write(MemoryBuffer buffer, Long value) {
      buffer.writeLong(value);
    }

    @Override
    public Long read(MemoryBuffer buffer) {
      return buffer.readLong();
    }
  }

  public static final class FloatSerializer extends Serializer<Float> {
    public FloatSerializer(RaySerde raySerDe) {
      super(raySerDe, Float.class);
    }

    @Override
    public void write(MemoryBuffer buffer, Float value) {
      buffer.writeFloat(value);
    }

    @Override
    public Float read(MemoryBuffer buffer) {
      return buffer.readFloat();
    }
  }

  public static final class DoubleSerializer extends Serializer<Double> {
    public DoubleSerializer(RaySerde raySerDe) {
      super(raySerDe, Double.class);
    }

    @Override
    public void write(MemoryBuffer buffer, Double value) {
      buffer.writeDouble(value);
    }

    @Override
    public Double read(MemoryBuffer buffer) {
      return buffer.readDouble();
    }
  }

  public static final class LocalDateSerializer extends Serializer<LocalDate> {
    public LocalDateSerializer(RaySerde raySerDe) {
      super(raySerDe, LocalDate.class);
    }

    @Override
    public void write(MemoryBuffer buffer, LocalDate value) {
      buffer.writeInt(value.getYear());
      buffer.writeByte((byte) value.getMonthValue());
      buffer.writeByte((byte) value.getDayOfMonth());
    }

    @Override
    public LocalDate read(MemoryBuffer buffer) {
      return LocalDate.of(buffer.readInt(), buffer.readByte(), buffer.readByte());
    }
  }

  public static final class DateSerializer extends Serializer<Date> {
    public DateSerializer(RaySerde raySerDe) {
      super(raySerDe, Date.class);
    }

    @Override
    public void write(MemoryBuffer buffer, Date value) {
      buffer.writeLong(value.getTime());
    }

    @Override
    public Date read(MemoryBuffer buffer) {
      return new Date(buffer.readLong());
    }
  }

  public static final class TimestampSerializer extends Serializer<Timestamp> {
    public TimestampSerializer(RaySerde raySerDe) {
      super(raySerDe, Timestamp.class);
    }

    @Override
    public void write(MemoryBuffer buffer, Timestamp value) {
      buffer.writeLong(value.getTime());
    }

    @Override
    public Timestamp read(MemoryBuffer buffer) {
      return new Timestamp(buffer.readLong());
    }
  }

  public static final class InstantSerializer extends Serializer<Instant> {
    public InstantSerializer(RaySerde raySerDe) {
      super(raySerDe, Instant.class);
    }

    @Override
    public void write(MemoryBuffer buffer, Instant value) {
      buffer.writeLong(value.getEpochSecond());
      buffer.writeInt(value.getNano());
    }

    @Override
    public Instant read(MemoryBuffer buffer) {
      return Instant.ofEpochSecond(buffer.readLong(), buffer.readInt());
    }
  }

  public static final class StringBuilderSerializer extends Serializer<StringBuilder> {
    private final StringSerializer stringSerializer;

    public StringBuilderSerializer(RaySerde raySerDe) {
      super(raySerDe, StringBuilder.class);
      stringSerializer = new StringSerializer(raySerDe);
    }

    @Override
    public void write(MemoryBuffer buffer, StringBuilder value) {
      stringSerializer.writeJavaString(buffer, value.toString());
    }

    @Override
    public StringBuilder read(MemoryBuffer buffer) {
      return new StringBuilder(stringSerializer.readJavaString(buffer));
    }
  }

  public static final class StringBufferSerializer extends Serializer<StringBuffer> {
    private final StringSerializer stringSerializer;

    public StringBufferSerializer(RaySerde raySerDe) {
      super(raySerDe, StringBuffer.class);
      stringSerializer = new StringSerializer(raySerDe);
    }

    @Override
    public void write(MemoryBuffer buffer, StringBuffer value) {
      stringSerializer.writeJavaString(buffer, value.toString());
    }

    @Override
    public StringBuffer read(MemoryBuffer buffer) {
      return new StringBuffer(stringSerializer.readJavaString(buffer));
    }
  }

  @SuppressWarnings("rawtypes")
  public static final class EnumSerializer extends Serializer<Enum> {
    private final Enum[] enumConstants;

    public EnumSerializer(RaySerde raySerDe, Class<Enum> cls) {
      super(raySerDe, cls);
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
    public void write(MemoryBuffer buffer, Enum value) {
      buffer.writeInt(value.ordinal());
    }

    @Override
    public Enum read(MemoryBuffer buffer) {
      return enumConstants[buffer.readInt()];
    }
  }

  public static final class BigDecimalSerializer extends Serializer<BigDecimal> {
    public BigDecimalSerializer(RaySerde raySerDe) {
      super(raySerDe, BigDecimal.class);
    }

    @Override
    public void write(MemoryBuffer buffer, BigDecimal value) {
      final byte[] bytes = value.unscaledValue().toByteArray();
      Preconditions.checkArgument(bytes.length <= 16);
      buffer.writeByte((byte) value.scale());
      buffer.writeByte((byte) bytes.length);
      buffer.writeBytes(bytes);
    }

    @Override
    public BigDecimal read(MemoryBuffer buffer) {
      int scale = buffer.readByte();
      int len = buffer.readByte();
      byte[] bytes = buffer.readBytes(len);
      final BigInteger bigInteger = new BigInteger(bytes);
      return new BigDecimal(bigInteger, scale);
    }
  }

  public static final class BigIntegerSerializer extends Serializer<BigInteger> {
    public BigIntegerSerializer(RaySerde raySerDe) {
      super(raySerDe, BigInteger.class);
    }

    @Override
    public void write(MemoryBuffer buffer, BigInteger value) {
      final byte[] bytes = value.toByteArray();
      Preconditions.checkArgument(bytes.length <= 16);
      buffer.writeByte((byte) bytes.length);
      buffer.writeBytes(bytes);
    }

    @Override
    public BigInteger read(MemoryBuffer buffer) {
      int len = buffer.readByte();
      byte[] bytes = buffer.readBytes(len);
      return new BigInteger(bytes);
    }
  }

  public static final class BooleanArraySerializer extends Serializer<boolean[]> {
    public BooleanArraySerializer(RaySerde raySerDe) {
      super(raySerDe, boolean[].class);
    }

    @Override
    public void write(MemoryBuffer buffer, boolean[] value) {
      writePrimitiveArray(buffer, value, Platform.BOOLEAN_ARRAY_OFFSET, value.length, 1);
    }

    @Override
    public boolean[] read(MemoryBuffer buffer) {
      int size = buffer.readInt();
      boolean[] values = new boolean[size];
      buffer.copyToUnsafe(buffer.readerIndex(), values, Platform.BOOLEAN_ARRAY_OFFSET, size);
      buffer.readerIndex(buffer.readerIndex() + size);
      return values;
    }
  }

  public static final class ByteArraySerializer extends Serializer<byte[]> {
    public ByteArraySerializer(RaySerde raySerDe) {
      super(raySerDe, byte[].class);
    }

    @Override
    public void write(MemoryBuffer buffer, byte[] value) {
      raySerde.writeSerializedObject(buffer, new SerializedObject.ByteArraySerializedObject(value));
    }

    @Override
    public byte[] read(MemoryBuffer buffer) {
      ByteBuffer buf = raySerde.readSerializedObject(buffer);
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
    public CharArraySerializer(RaySerde raySerDe) {
      super(raySerDe, char[].class);
    }

    @Override
    public void write(MemoryBuffer buffer, char[] value) {
      writePrimitiveArray(buffer, value, Platform.CHAR_ARRAY_OFFSET, value.length, 2);
    }

    @Override
    public char[] read(MemoryBuffer buffer) {
      int size = buffer.readInt();
      int numElements = size / 2;
      char[] values = new char[numElements];
      buffer.copyToUnsafe(buffer.readerIndex(), values, Platform.CHAR_ARRAY_OFFSET, size);
      buffer.readerIndex(buffer.readerIndex() + size);
      return values;
    }
  }

  public static final class ShortArraySerializer extends Serializer<short[]> {
    public ShortArraySerializer(RaySerde raySerDe) {
      super(raySerDe, short[].class);
    }

    @Override
    public void write(MemoryBuffer buffer, short[] value) {
      writePrimitiveArray(buffer, value, Platform.SHORT_ARRAY_OFFSET, value.length, 2);
    }

    @Override
    public short[] read(MemoryBuffer buffer) {
      int size = buffer.readInt();
      int numElements = size / 2;
      short[] values = new short[numElements];
      buffer.copyToUnsafe(buffer.readerIndex(), values, Platform.SHORT_ARRAY_OFFSET, size);
      buffer.readerIndex(buffer.readerIndex() + size);
      return values;
    }
  }

  public static final class IntArraySerializer extends Serializer<int[]> {
    public IntArraySerializer(RaySerde raySerDe) {
      super(raySerDe, int[].class);
    }

    @Override
    public void write(MemoryBuffer buffer, int[] value) {
      writePrimitiveArray(buffer, value, Platform.INT_ARRAY_OFFSET, value.length, 4);
    }

    @Override
    public int[] read(MemoryBuffer buffer) {
      int size = buffer.readInt();
      int numElements = size / 4;
      int[] values = new int[numElements];
      buffer.copyToUnsafe(buffer.readerIndex(), values, Platform.INT_ARRAY_OFFSET, size);
      buffer.readerIndex(buffer.readerIndex() + size);
      return values;
    }
  }

  public static final class LongArraySerializer extends Serializer<long[]> {
    public LongArraySerializer(RaySerde raySerDe) {
      super(raySerDe, long[].class);
    }

    @Override
    public void write(MemoryBuffer buffer, long[] value) {
      writePrimitiveArray(buffer, value, Platform.LONG_ARRAY_OFFSET, value.length, 8);
    }

    @Override
    public long[] read(MemoryBuffer buffer) {
      int size = buffer.readInt();
      int numElements = size / 8;
      long[] values = new long[numElements];
      buffer.copyToUnsafe(buffer.readerIndex(), values, Platform.LONG_ARRAY_OFFSET, size);
      buffer.readerIndex(buffer.readerIndex() + size);
      return values;
    }
  }

  public static final class FloatArraySerializer extends Serializer<float[]> {
    public FloatArraySerializer(RaySerde raySerDe) {
      super(raySerDe, float[].class);
    }

    @Override
    public void write(MemoryBuffer buffer, float[] value) {
      writePrimitiveArray(buffer, value, Platform.FLOAT_ARRAY_OFFSET, value.length, 4);
    }

    @Override
    public float[] read(MemoryBuffer buffer) {
      int size = buffer.readInt();
      int numElements = size / 4;
      float[] values = new float[numElements];
      buffer.copyToUnsafe(buffer.readerIndex(), values, Platform.FLOAT_ARRAY_OFFSET, size);
      buffer.readerIndex(buffer.readerIndex() + size);
      return values;
    }
  }

  public static final class DoubleArraySerializer extends Serializer<double[]> {
    public DoubleArraySerializer(RaySerde raySerDe) {
      super(raySerDe, double[].class);
    }

    @Override
    public void write(MemoryBuffer buffer, double[] value) {
      writePrimitiveArray(buffer, value, Platform.DOUBLE_ARRAY_OFFSET, value.length, 8);
    }

    @Override
    public double[] read(MemoryBuffer buffer) {
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
    private final ReferenceResolver referenceResolver;

    public StringArraySerializer(RaySerde raySerDe) {
      super(raySerDe, String[].class);
      stringSerializer = new StringSerializer(raySerDe);
      referenceResolver = raySerDe.getReferenceResolver();
    }

    @Override
    public void write(MemoryBuffer buffer, String[] value) {
      int len = value.length;
      buffer.writeInt(len);
      for (String elem : value) {
        raySerde.serializeReferencableToJava(buffer, elem, stringSerializer);
      }
    }

    @Override
    public String[] read(MemoryBuffer buffer) {
      int numElements = buffer.readInt();
      String[] value = new String[numElements];
      referenceResolver.reference(value);
      for (int i = 0; i < numElements; i++) {
        String elem = raySerde.deserializeReferencableFromJava(buffer, stringSerializer);
        value[i] = elem;
      }
      return value;
    }
  }

  /** May be multi-dimension array, or multi-dimension primitive array. */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static final class ObjectArraySerializer<T> extends Serializer<T[]> {
    private final Class<T> innerType;
    private final Serializer<T> componentTypeSerializer;
    private final int[] stubDims;

    public ObjectArraySerializer(RaySerde raySerDe, Class<T[]> cls) {
      super(raySerDe, cls);
      Tuple2<Class<?>, Integer> arrayComponentInfo = TypeUtils.getArrayComponentInfo(cls);
      int dimension = arrayComponentInfo.f1;
      this.innerType = (Class<T>) arrayComponentInfo.f0;
      Class<?> componentType = cls.getComponentType();
      if (Modifier.isFinal(componentType.getModifiers())) {
        this.componentTypeSerializer =
            (Serializer<T>) raySerDe.getClassResolver().getSerializer(componentType);
      } else {
        this.componentTypeSerializer = null;
      }
      this.stubDims = new int[dimension];
    }

    @Override
    public void write(MemoryBuffer buffer, T[] arr) {
      int len = arr.length;
      buffer.writeInt(len);
      final Serializer<T> componentTypeSerializer = this.componentTypeSerializer;
      if (componentTypeSerializer != null) {
        for (T t : arr) {
          raySerde.serializeReferencableToJava(buffer, t, componentTypeSerializer);
        }
      } else {
        for (T t : arr) {
          raySerde.serializeReferencableToJava(buffer, t);
        }
      }
    }

    @Override
    public T[] read(MemoryBuffer buffer) {
      int numElements = buffer.readInt();
      Object[] value = newArray(numElements);
      ReferenceResolver referenceResolver = raySerde.getReferenceResolver();
      referenceResolver.reference(value);
      @SuppressWarnings("rawtypes")
      final Serializer componentTypeSerializer = this.componentTypeSerializer;
      if (componentTypeSerializer != null) {
        for (int i = 0; i < numElements; i++) {
          value[i] = raySerde.deserializeReferencableFromJava(buffer, componentTypeSerializer);
        }
      } else {
        for (int i = 0; i < numElements; i++) {
          value[i] = raySerde.deserializeReferencableFromJava(buffer);
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

  @SuppressWarnings("rawtypes")
  public static final class ClassSerializer extends Serializer<Class> {
    private static final byte USE_CLASS_ID = 0;
    private static final byte USE_CLASSNAME = 1;
    private static final byte PRIMITIVE_FLAG = 2;
    private final IdentityHashMap<Class<?>, Byte> primitivesMap = new IdentityHashMap<>();
    private final Class<?>[] id2PrimitiveClasses = new Class[Primitives.allPrimitiveTypes().size()];

    public ClassSerializer(RaySerde raySerDe) {
      super(raySerDe, Class.class);
      byte count = 0;
      for (Class<?> primitiveType : Primitives.allPrimitiveTypes()) {
        primitivesMap.put(primitiveType, count);
        id2PrimitiveClasses[count] = primitiveType;
        count++;
      }
    }

    @Override
    public void write(MemoryBuffer buffer, Class value) {
      Short classId = raySerde.getClassResolver().getRegisteredClassId(value);
      if (classId != null) {
        buffer.writeByte(USE_CLASS_ID);
        buffer.writeShort(classId);
      } else {
        if (value.isPrimitive()) {
          buffer.writeByte(PRIMITIVE_FLAG);
          buffer.writeByte(primitivesMap.get(value));
        } else {
          buffer.writeByte(USE_CLASSNAME);
          raySerde.getClassResolver().writeClassNameBytes(buffer, value);
        }
      }
    }

    @Override
    public Class read(MemoryBuffer buffer) {
      byte tag = buffer.readByte();
      if (tag == USE_CLASS_ID) {
        return raySerde.getClassResolver().getRegisteredClass(buffer.readShort());
      } else {
        if (tag == PRIMITIVE_FLAG) {
          return id2PrimitiveClasses[buffer.readByte()];
        } else {
          return raySerde.getClassResolver().readClassByClassNameBytes(buffer);
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

    public ByteBufferSerializer(RaySerde raySerDe, Class<ByteBuffer> cls) {
      super(raySerDe, cls);
    }

    @Override
    public void write(MemoryBuffer buffer, ByteBuffer value) {
      raySerde.writeSerializedObject(
          buffer, new SerializedObject.ByteBufferSerializedObject(value));
    }

    @Override
    public ByteBuffer read(MemoryBuffer buffer) {
      return raySerde.readSerializedObject(buffer);
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

  public static void checkClassVersion(RaySerde raySerDe, int readHash, int classVersionHash) {
    if (readHash != classVersionHash) {
      throw new SerdeException(
          String.format(
              "Read class %s version %s is not consistent with %s",
              raySerDe.getClassResolver().getCurrentReadClass(), readHash, classVersionHash));
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
