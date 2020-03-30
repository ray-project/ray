package org.ray.runtime.serializer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.ExtensionValue;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

// We can't pack List / Map by MessagePack, because we don't know the type class when unpacking.
public class MessagePackSerializer {

  private static final byte CROSS_LANGUAGE_TYPE_EXTENSION_ID = 100;
  private static final byte LANGUAGE_SPECIFIC_TYPE_EXTENSION_ID = 101;
  // MessagePack length is an int takes up to 9 bytes.
  // https://github.com/msgpack/msgpack/blob/master/spec.md#int-format-family
  private static final int MESSAGE_PACK_OFFSET = 9;

  private static Map<Class<?>, Type> mapSerializer = new HashMap<>();
  private static Map<ValueType, Map<Class<?>, Type>> mapDeserializer = new HashMap<>();
  private static ExtensionType extensionType = new ExtensionType();

  static {
    // Register types to mapSerializer and mapDeserializer.
    new NullType();
    new BooleanType();
    new ByteType();
    new ShortType();
    new IntType();
    new LongType();
    new BigIntegerType();
    new FloatType();
    new DoubleType();
    new StringType();
    new BinaryType();
    new ArrayType();
    new ExtensionType();

    // null / Array / Extension always use special type class as key.
    Preconditions.checkState(
        mapDeserializer.get(ValueType.BOOLEAN).get(Object.class) instanceof BooleanType);
    Preconditions.checkState(
        mapDeserializer.get(ValueType.INTEGER).get(Object.class) instanceof IntType);
    Preconditions.checkState(
        mapDeserializer.get(ValueType.FLOAT).get(Object.class) instanceof DoubleType);
    Preconditions.checkState(
        mapDeserializer.get(ValueType.STRING).get(Object.class) instanceof StringType);
    Preconditions.checkState(
        mapDeserializer.get(ValueType.BINARY).get(Object.class) instanceof BinaryType);
  }

  interface JavaSerializer {

    void serialize(Object object, MessagePacker packer) throws IOException;
  }

  interface JavaDeserializer {

    Object deserialize(ExtensionValue v);
  }

  private interface Type {

    void pack(Object object, MessagePacker packer,
        JavaSerializer javaSerializer) throws IOException;

    Object unpack(Value v, Class<?> type,
        JavaDeserializer javaDeserializer);
  }

  private static class NullType implements Type {

    public NullType() {
      MessagePackSerializer.mapSerializer.put(NullType.class, this);
      MessagePackSerializer.mapDeserializer
          .put(ValueType.NIL, ImmutableMap.of(NullType.class, this));
    }

    public void pack(Object object, MessagePacker packer,
        JavaSerializer javaSerializer) throws IOException {
      packer.packNil();
    }

    public Object unpack(Value v, Class<?> type, JavaDeserializer javaDeserializer) {
      return null;
    }
  }

  private static class BooleanType implements Type {

    public BooleanType() {
      MessagePackSerializer.mapSerializer.put(Boolean.class, this);
      MessagePackSerializer.mapDeserializer
          .put(ValueType.BOOLEAN, ImmutableMap.of(
              Boolean.class, this,
              boolean.class, this,
              Object.class, this));
    }

    public void pack(Object object, MessagePacker packer,
        JavaSerializer javaSerializer) throws IOException {
      packer.packBoolean((Boolean) object);
    }

    public Object unpack(Value v, Class<?> type, JavaDeserializer javaDeserializer) {
      return v.asBooleanValue().getBoolean();
    }
  }

  private static class ByteType implements Type {

    public ByteType() {
      MessagePackSerializer.mapSerializer.put(Byte.class, this);
      Map<Class<?>, Type> value = ImmutableMap.of(
          Byte.class, this,
          byte.class, this);
      MessagePackSerializer.mapDeserializer.putIfAbsent(ValueType.INTEGER, new HashMap<>());
      MessagePackSerializer.mapDeserializer.get(ValueType.INTEGER).putAll(value);
    }

    public void pack(Object object, MessagePacker packer,
        JavaSerializer javaSerializer) throws IOException {
      packer.packByte((Byte) object);
    }

    public Object unpack(Value v, Class<?> type, JavaDeserializer javaDeserializer) {
      IntegerValue iv = v.asIntegerValue();
      Preconditions.checkState(iv.isInByteRange());
      return iv.asByte();
    }
  }

  private static class ShortType implements Type {

    public ShortType() {
      MessagePackSerializer.mapSerializer.put(Short.class, this);
      Map<Class<?>, Type> value = ImmutableMap.of(
          Short.class, this,
          short.class, this);
      MessagePackSerializer.mapDeserializer.putIfAbsent(ValueType.INTEGER, new HashMap<>());
      MessagePackSerializer.mapDeserializer.get(ValueType.INTEGER).putAll(value);
    }

    public void pack(Object object, MessagePacker packer,
        JavaSerializer javaSerializer) throws IOException {
      packer.packShort((Short) object);
    }

    public Object unpack(Value v, Class<?> type, JavaDeserializer javaDeserializer) {
      IntegerValue iv = v.asIntegerValue();
      Preconditions.checkState(iv.isInShortRange());
      return iv.asShort();
    }
  }

  private static class IntType implements Type {

    public IntType() {
      MessagePackSerializer.mapSerializer.put(Integer.class, this);
      Map<Class<?>, Type> value = ImmutableMap.of(
          Integer.class, this,
          int.class, this,
          Object.class, this);
      MessagePackSerializer.mapDeserializer.putIfAbsent(ValueType.INTEGER, new HashMap<>());
      MessagePackSerializer.mapDeserializer.get(ValueType.INTEGER).putAll(value);
    }

    public void pack(Object object, MessagePacker packer,
        JavaSerializer javaSerializer) throws IOException {
      packer.packInt((Integer) object);
    }

    public Object unpack(Value v, Class<?> type, JavaDeserializer javaDeserializer) {
      IntegerValue iv = v.asIntegerValue();
      Preconditions.checkState(iv.isInIntRange());
      return iv.asInt();
    }
  }

  private static class LongType implements Type {

    public LongType() {
      MessagePackSerializer.mapSerializer.put(Long.class, this);
      Map<Class<?>, Type> value = ImmutableMap.of(
          Long.class, this,
          long.class, this);
      MessagePackSerializer.mapDeserializer.putIfAbsent(ValueType.INTEGER, new HashMap<>());
      MessagePackSerializer.mapDeserializer.get(ValueType.INTEGER).putAll(value);
    }

    public void pack(Object object, MessagePacker packer,
        JavaSerializer javaSerializer) throws IOException {
      packer.packLong((Long) object);
    }

    public Object unpack(Value v, Class<?> type, JavaDeserializer javaDeserializer) {
      IntegerValue iv = v.asIntegerValue();
      Preconditions.checkState(iv.isInLongRange());
      return iv.asLong();
    }
  }

  private static class BigIntegerType implements Type {

    public BigIntegerType() {
      MessagePackSerializer.mapSerializer.put(BigInteger.class, this);
      Map<Class<?>, Type> value = ImmutableMap.of(BigInteger.class, this);
      MessagePackSerializer.mapDeserializer.putIfAbsent(ValueType.INTEGER, new HashMap<>());
      MessagePackSerializer.mapDeserializer.get(ValueType.INTEGER).putAll(value);
    }

    public void pack(Object object, MessagePacker packer,
        JavaSerializer javaSerializer) throws IOException {
      packer.packBigInteger((BigInteger) object);
    }

    public Object unpack(Value v, Class<?> type, JavaDeserializer javaDeserializer) {
      return v.asIntegerValue().asBigInteger();
    }
  }

  private static class FloatType implements Type {

    public FloatType() {
      MessagePackSerializer.mapSerializer.put(Float.class, this);
      Map<Class<?>, Type> value = ImmutableMap.of(
          Float.class, this,
          float.class, this);
      MessagePackSerializer.mapDeserializer.putIfAbsent(ValueType.FLOAT, new HashMap<>());
      MessagePackSerializer.mapDeserializer.get(ValueType.FLOAT).putAll(value);
    }

    public void pack(Object object, MessagePacker packer,
        JavaSerializer javaSerializer) throws IOException {
      packer.packFloat((Float) object);
    }

    public Object unpack(Value v, Class<?> type, JavaDeserializer javaDeserializer) {
      return v.asFloatValue().toFloat();
    }
  }

  private static class DoubleType implements Type {

    public DoubleType() {
      MessagePackSerializer.mapSerializer.put(Double.class, this);
      Map<Class<?>, Type> value = ImmutableMap.of(
          Double.class, this,
          double.class, this,
          Object.class, this);
      MessagePackSerializer.mapDeserializer.putIfAbsent(ValueType.FLOAT, new HashMap<>());
      MessagePackSerializer.mapDeserializer.get(ValueType.FLOAT).putAll(value);
    }

    public void pack(Object object, MessagePacker packer,
        JavaSerializer javaSerializer) throws IOException {
      packer.packDouble((Double) object);
    }

    public Object unpack(Value v, Class<?> type, JavaDeserializer javaDeserializer) {
      return v.asFloatValue().toDouble();
    }
  }

  private static class StringType implements Type {

    public StringType() {
      MessagePackSerializer.mapSerializer.put(String.class, this);
      MessagePackSerializer.mapDeserializer
          .put(ValueType.STRING, ImmutableMap.of(
              String.class, this,
              Object.class, this));
    }

    public void pack(Object object, MessagePacker packer,
        JavaSerializer javaSerializer) throws IOException {
      packer.packString((String) object);
    }

    public Object unpack(Value v, Class<?> type, JavaDeserializer javaDeserializer) {
      return v.asStringValue().asString();
    }
  }

  private static class BinaryType implements Type {

    public BinaryType() {
      MessagePackSerializer.mapSerializer.put(byte[].class, this);
      MessagePackSerializer.mapDeserializer
          .put(ValueType.BINARY, ImmutableMap.of(
              byte[].class, this,
              Object.class, this));
    }

    public void pack(Object object, MessagePacker packer,
        JavaSerializer javaSerializer) throws IOException {
      byte[] bytes = (byte[]) object;
      packer.packBinaryHeader(bytes.length);
      packer.writePayload(bytes);
    }

    public Object unpack(Value v, Class<?> type, JavaDeserializer javaDeserializer) {
      return v.asBinaryValue().asByteArray();
    }
  }

  private static class ArrayType implements Type {

    public ArrayType() {
      MessagePackSerializer.mapSerializer.put(ArrayType.class, this);
      MessagePackSerializer.mapDeserializer
          .put(ValueType.ARRAY, ImmutableMap.of(ArrayType.class, this));
    }

    public void pack(Object object, MessagePacker packer,
        JavaSerializer javaSerializer) throws IOException {
      int length = Array.getLength(object);
      packer.packArrayHeader(length);
      for (int i = 0; i < length; ++i) {
        MessagePackSerializer.pack(Array.get(object, i), packer, javaSerializer);
      }
    }

    public Object unpack(Value v, Class<?> type, JavaDeserializer javaDeserializer) {
      ArrayValue a = v.asArrayValue();
      Class<?> componentType = type.isArray() ? type.getComponentType() : Object.class;
      Object array = Array.newInstance(componentType, a.size());
      for (int i = 0; i < a.size(); ++i) {
        Value value = a.get(i);
        Array.set(array, i, MessagePackSerializer.unpack(value, componentType, javaDeserializer));
      }
      return array;
    }
  }

  private static class ExtensionType implements Type {

    public ExtensionType() {
      MessagePackSerializer.mapDeserializer
          .put(ValueType.EXTENSION, ImmutableMap.of(ExtensionType.class, this));
    }

    public void pack(Object object, MessagePacker packer,
        JavaSerializer javaSerializer) throws IOException {
      try {
        // Get type id and cross data from object.
        Class<?> cls = object.getClass();
        Method crossTypeId = cls.getDeclaredMethod(CrossTypeManager.KEY_CROSS_TYPE_ID);
        crossTypeId.setAccessible(true);
        Method toCrossData = cls.getDeclaredMethod(CrossTypeManager.KEY_TO_CROSS_DATA);
        toCrossData.setAccessible(true);
        // Serialize [type id, cross data] by MessagePack.
        Object data = new Object[]{
            crossTypeId.invoke(null),
            toCrossData.invoke(object),
        };
        MessageBufferPacker crossTypePacker = MessagePack.newDefaultBufferPacker();
        MessagePackSerializer.pack(data, crossTypePacker, javaSerializer);
        // Put the serialized bytes to CROSS_LANGUAGE_TYPE_EXTENSION_ID.
        byte[] payload = crossTypePacker.toByteArray();
        packer.packExtensionTypeHeader(CROSS_LANGUAGE_TYPE_EXTENSION_ID, payload.length);
        packer.addPayload(payload);
      } catch (Exception e) {
        javaSerializer.serialize(object, packer);
      }
    }

    public Object unpack(Value v, Class<?> type, JavaDeserializer javaDeserializer) {
      ExtensionValue ev = v.asExtensionValue();
      byte extType = ev.getType();
      if (extType == CROSS_LANGUAGE_TYPE_EXTENSION_ID) {
        try {
          MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(ev.getData());
          Value crossValue = unpacker.unpackValue();
          // data is [type id, cross data object]
          Object[] data = (Object[]) MessagePackSerializer
              .unpack(crossValue, Object[].class, javaDeserializer);
          Preconditions.checkState(data != null && data.length == 2);
          Integer crossTypeId = ((Number) data[0]).intValue();
          Object[] crossData = (Object[]) data[1];
          // Get type by type id and call KEY_FROM_CROSS_DATA method with cross data.
          Class<?> crossType = CrossTypeManager.get(crossTypeId);
          Method fromCrossData = crossType.getDeclaredMethod(
              CrossTypeManager.KEY_FROM_CROSS_DATA, Object[].class);
          fromCrossData.setAccessible(true);
          return fromCrossData.invoke(null, new Object[]{crossData});
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      } else if (extType == LANGUAGE_SPECIFIC_TYPE_EXTENSION_ID) {
        return javaDeserializer.deserialize(ev);
      }
      return null;
    }
  }

  private static void pack(Object object, MessagePacker packer, JavaSerializer javaSerializer)
      throws IOException {
    Class<?> type;
    if (object == null) {
      type = NullType.class;
    } else if (object.getClass().isArray()) {
      type = ArrayType.class;
    } else {
      type = object.getClass();
    }
    Type t = mapSerializer.get(type);
    if (t != null) {
      t.pack(object, packer, javaSerializer);
    } else {
      extensionType.pack(object, packer, javaSerializer);
    }
  }

  private static Object unpack(Value v, Class<?> type, JavaDeserializer javaDeserializer) {
    ValueType valueType = v.getValueType();
    Class<?> typeKey;
    if (valueType == ValueType.NIL) {
      typeKey = NullType.class;
    } else if (valueType == ValueType.ARRAY) {
      typeKey = ArrayType.class;
    } else if (valueType == ValueType.EXTENSION) {
      typeKey = ExtensionType.class;
    } else {
      typeKey = type;
    }
    try {
      return mapDeserializer.get(valueType).get(typeKey).unpack(v, type, javaDeserializer);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Can't unpack value %s with type %s", v.toString(), type.toString()));
    }
  }

  public static byte[] encode(Object obj, Serializer.Meta meta, ClassLoader classLoader) {
    MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
    try {
      // Reserve MESSAGE_PACK_OFFSET bytes for MessagePack bytes length.
      packer.writePayload(new byte[MESSAGE_PACK_OFFSET]);
      // Serialize input object by MessagePack.
      Serializer.Meta javaEncoderMeta = new Serializer.Meta();
      pack(obj, packer, ((object, packer1) -> {
        byte[] payload = FstSerializer.encode(object, javaEncoderMeta, classLoader);
        packer1.packExtensionTypeHeader(LANGUAGE_SPECIFIC_TYPE_EXTENSION_ID, payload.length);
        packer1.addPayload(payload);
        meta.isCrossLanguage = false;
      }));
      byte[] msgpackBytes = packer.toByteArray();
      // Serialize MessagePack bytes length.
      MessageBufferPacker headerPacker = MessagePack.newDefaultBufferPacker();
      Preconditions.checkState(msgpackBytes.length >= MESSAGE_PACK_OFFSET);
      headerPacker.packLong(msgpackBytes.length - MESSAGE_PACK_OFFSET);
      byte[] msgpackBytesLength = headerPacker.toByteArray();
      // Check serialized MessagePack bytes length is valid.
      Preconditions.checkState(msgpackBytesLength.length <= MESSAGE_PACK_OFFSET);
      // Write MessagePack bytes length to reserved buffer.
      System.arraycopy(msgpackBytesLength, 0, msgpackBytes, 0, msgpackBytesLength.length);
      return msgpackBytes;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  @SuppressWarnings("unchecked")
  public static <T> T decode(byte[] bs, Class<?> type, ClassLoader classLoader) {
    try {
      // Read MessagePack bytes length.
      MessageUnpacker headerUnpacker = MessagePack.newDefaultUnpacker(bs, 0, MESSAGE_PACK_OFFSET);
      Long msgpackBytesLength = headerUnpacker.unpackLong();
      headerUnpacker.close();
      // Check MessagePack bytes length is valid.
      Preconditions.checkState(MESSAGE_PACK_OFFSET + msgpackBytesLength <= bs.length);
      // Deserialize MessagePack bytes from MESSAGE_PACK_OFFSET.
      MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bs, MESSAGE_PACK_OFFSET,
          msgpackBytesLength.intValue());
      Value v = unpacker.unpackValue();
      if (type == null) {
        type = Object.class;
      }
      return (T) unpack(v, type, ((ExtensionValue ev) -> FstSerializer.decode(ev.getData(),
          classLoader)));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void setClassloader(ClassLoader classLoader) {
    FstSerializer.setClassloader(classLoader);
  }
}
