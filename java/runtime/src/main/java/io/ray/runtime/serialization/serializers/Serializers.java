package io.ray.runtime.serialization.serializers;

import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Primitives;
import io.ray.runtime.io.MemoryBuffer;
import io.ray.runtime.io.Platform;
import io.ray.runtime.serialization.RaySerde;
import io.ray.runtime.serialization.SerdeException;
import io.ray.runtime.serialization.SerializedObject;
import io.ray.runtime.serialization.Serializer;
import io.ray.runtime.serialization.resolver.ReferenceResolver;
import io.ray.runtime.serialization.util.Descriptor;
import io.ray.runtime.serialization.util.Tuple2;
import io.ray.runtime.serialization.util.TypeUtils;
import java.lang.reflect.Array;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
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
  public static final class PrimitiveArraySerializer extends Serializer {
    private final Class<?> innerType;
    private final int offset;
    private final int elemSize;

    public PrimitiveArraySerializer(RaySerde raySerDe, Class<?> cls) {
      super(raySerDe, cls);
      this.innerType = TypeUtils.getArrayComponentInfo(cls).f0;
      this.offset = getPrimitiveInfo().get(innerType).f0;
      this.elemSize = getPrimitiveInfo().get(innerType).f1;
    }

    @Override
    public void write(MemoryBuffer buffer, Object value) {
      raySerde.writeSerializedObject(
          buffer, new PrimitiveArraySerializedObject(value, offset, elemSize));
    }

    @Override
    public Object read(MemoryBuffer buffer) {
      ByteBuffer buf = raySerde.readSerializedObject(buffer);
      int size = buf.getInt();
      int numElements = size / elemSize;
      Object values = Array.newInstance(innerType, numElements);
      MemoryBuffer.fromByteBuffer(buf).copyToUnsafe(0, values, offset, size);
      return values;
    }
  }

  public static class PrimitiveArraySerializedObject implements SerializedObject {
    private final Object array;
    private final int offset;
    private final int elemSize;

    public PrimitiveArraySerializedObject(Object array, int offset, int elemSize) {
      this.array = array;
      this.offset = offset;
      this.elemSize = elemSize;
    }

    @Override
    public int totalBytes() {
      return 4 + Array.getLength(array) * elemSize;
    }

    @Override
    public void writeTo(MemoryBuffer buffer) {
      writePrimitiveArray(buffer, array, offset, Array.getLength(array), elemSize);
    }

    @Override
    public ByteBuffer toBuffer() {
      MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(8);
      writeTo(buffer);
      return buffer.sliceAsByteBuffer(0, buffer.writerIndex());
    }
  }

  public static final class ByteArraySerializer extends Serializer<byte[]> {
    public ByteArraySerializer(RaySerde raySerDe) {
      super(raySerDe, byte[].class);
    }

    @Override
    public void write(MemoryBuffer buffer, byte[] value) {
      raySerde.writeSerializedObject(buffer, new ByteArraySerializedObject(value));
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

  public static class ByteArraySerializedObject implements SerializedObject {
    private final byte[] bytes;

    public ByteArraySerializedObject(byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public int totalBytes() {
      return bytes.length;
    }

    @Override
    public void writeTo(MemoryBuffer buffer) {
      buffer.writeBytes(bytes);
    }

    @Override
    public ByteBuffer toBuffer() {
      return ByteBuffer.wrap(bytes);
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

  private static IdentityHashMap<Class<?>, Tuple2<Integer, Integer>> getPrimitiveInfo() {
    IdentityHashMap<Class<?>, Tuple2<Integer, Integer>> primitiveInfo = new IdentityHashMap<>();
    primitiveInfo.put(boolean.class, Tuple2.of(Platform.BOOLEAN_ARRAY_OFFSET, 1));
    primitiveInfo.put(byte.class, Tuple2.of(Platform.BYTE_ARRAY_OFFSET, 1));
    primitiveInfo.put(char.class, Tuple2.of(Platform.CHAR_ARRAY_OFFSET, 2));
    primitiveInfo.put(short.class, Tuple2.of(Platform.SHORT_ARRAY_OFFSET, 2));
    primitiveInfo.put(int.class, Tuple2.of(Platform.INT_ARRAY_OFFSET, 4));
    primitiveInfo.put(long.class, Tuple2.of(Platform.LONG_ARRAY_OFFSET, 8));
    primitiveInfo.put(float.class, Tuple2.of(Platform.FLOAT_ARRAY_OFFSET, 4));
    primitiveInfo.put(double.class, Tuple2.of(Platform.DOUBLE_ARRAY_OFFSET, 8));
    return primitiveInfo;
  }
}
