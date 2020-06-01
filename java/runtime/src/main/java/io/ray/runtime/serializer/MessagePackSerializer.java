package io.ray.runtime.serializer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
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

  private static final byte LANGUAGE_SPECIFIC_TYPE_EXTENSION_ID = 101;
  // MessagePack length is an int takes up to 9 bytes.
  // https://github.com/msgpack/msgpack/blob/master/spec.md#int-format-family
  private static final int MESSAGE_PACK_OFFSET = 9;

  // Pakcers indexed by its corresponding Java class object.
  private static Map<Class<?>, TypePacker> packers = new HashMap<>();
  // Unpackers indexed by its corresponding MessagePack ValueType.
  private static Map<ValueType, TypeUnpacker> unpackers = new HashMap<>();
  // Null and array don't have a corresponding class, so define them separately.
  private static final TypePacker NULL_PACKER;
  private static final TypePacker ARRAY_PACKER;
  private static final TypePacker EXTENSION_PACKER;

  static {
    // ===== Initialize packers =====
    // Null packer.
    NULL_PACKER = (object, packer, javaSerializer) -> packer.packNil();

    // Array packer.
    ARRAY_PACKER = ((object, packer, javaSerializer) -> {
      int length = Array.getLength(object);
      packer.packArrayHeader(length);
      for (int i = 0; i < length; ++i) {
        pack(Array.get(object, i), packer, javaSerializer);
      }
    });

    // Extension packer.
    EXTENSION_PACKER = ((object, packer, javaSerializer) -> {
      javaSerializer.serialize(object, packer);
    });

    packers.put(Boolean.class,
        ((object, packer, javaSerializer) -> packer.packBoolean((Boolean) object)));
    packers.put(Byte.class,
        ((object, packer, javaSerializer) -> packer.packByte((Byte) object)));
    packers.put(Short.class,
        ((object, packer, javaSerializer) -> packer.packShort((Short) object)));
    packers.put(Integer.class,
        ((object, packer, javaSerializer) -> packer.packInt((Integer) object)));
    packers.put(Long.class,
        ((object, packer, javaSerializer) -> packer.packLong((Long) object)));
    packers.put(BigInteger.class,
        ((object, packer, javaSerializer) -> packer.packBigInteger((BigInteger) object)));
    packers.put(Float.class,
        ((object, packer, javaSerializer) -> packer.packFloat((Float) object)));
    packers.put(Double.class,
        ((object, packer, javaSerializer) -> packer.packDouble((Double) object)));
    packers.put(String.class,
        ((object, packer, javaSerializer) -> packer.packString((String) object)));
    packers.put(byte[].class,
        ((object, packer, javaSerializer) -> {
          byte[] bytes = (byte[]) object;
          packer.packBinaryHeader(bytes.length);
          packer.writePayload(bytes);
        }));

    // ===== Initialize unpackers =====
    List<Class<?>> booleanClasses = ImmutableList.of(Boolean.class, boolean.class);
    List<Class<?>> byteClasses = ImmutableList.of(Byte.class, byte.class);
    List<Class<?>> shortClasses = ImmutableList.of(Short.class, short.class);
    List<Class<?>> intClasses = ImmutableList.of(Integer.class, int.class);
    List<Class<?>> longClasses = ImmutableList.of(Long.class, long.class);
    List<Class<?>> bigIntClasses = ImmutableList.of(BigInteger.class);
    List<Class<?>> floatClasses = ImmutableList.of(Float.class, float.class);
    List<Class<?>> doubleClasses = ImmutableList.of(Double.class, double.class);
    List<Class<?>> stringClasses = ImmutableList.of(String.class);
    List<Class<?>> binaryClasses = ImmutableList.of(byte[].class);

    // Null unpacker.
    unpackers.put(ValueType.NIL, (value, targetClass, javaDeserializer) -> null);
    // Boolean unpacker.
    unpackers.put(ValueType.BOOLEAN, (value, targetClass, javaDeserializer) -> {
      Preconditions.checkArgument(checkTypeCompatible(booleanClasses, targetClass),
          "Boolean can't be deserialized as {}.", targetClass);
      return value.asBooleanValue().getBoolean();
    });
    // Integer unpacker.
    unpackers.put(ValueType.INTEGER, ((value, targetClass, javaDeserializer) -> {
      IntegerValue iv = value.asIntegerValue();
      if (iv.isInByteRange() && checkTypeCompatible(byteClasses, targetClass)) {
        return iv.asByte();
      } else if (iv.isInShortRange() && checkTypeCompatible(shortClasses, targetClass)) {
        return iv.asShort();
      } else if (iv.isInIntRange() && checkTypeCompatible(intClasses, targetClass)) {
        return iv.asInt();
      } else if (iv.isInLongRange() && checkTypeCompatible(longClasses, targetClass)) {
        return iv.asLong();
      } else if (checkTypeCompatible(bigIntClasses, targetClass)) {
        return iv.asBigInteger();
      }
      throw new IllegalArgumentException("Integer can't be deserialized as " + targetClass + ".");
    }));
    // Float unpacker.
    unpackers.put(ValueType.FLOAT, ((value, targetClass, javaDeserializer) -> {
      if (checkTypeCompatible(doubleClasses, targetClass)) {
        return value.asFloatValue().toDouble();
      } else if (checkTypeCompatible(floatClasses, targetClass)) {
        return value.asFloatValue().toFloat();
      }
      throw new IllegalArgumentException("Float can't be deserialized as " + targetClass + ".");
    }));
    // String unpacker.
    unpackers.put(ValueType.STRING, ((value, targetClass, javaDeserializer) -> {
      Preconditions.checkArgument(checkTypeCompatible(stringClasses, targetClass),
          "String can't be deserialized as {}.", targetClass);
      return value.asStringValue().asString();
    }));
    // Binary unpacker.
    unpackers.put(ValueType.BINARY, ((value, targetClass, javaDeserializer) -> {
      Preconditions.checkArgument(checkTypeCompatible(binaryClasses, targetClass),
          "Binary can't be deserialized as {}.", targetClass);
      return value.asBinaryValue().asByteArray();
    }));
    // Array unpacker.
    unpackers.put(ValueType.ARRAY, ((value, targetClass, javaDeserializer) -> {
      ArrayValue av = value.asArrayValue();
      Class<?> componentType =
          targetClass.isArray() ? targetClass.getComponentType() : Object.class;
      Object array = Array.newInstance(componentType, av.size());
      for (int i = 0; i < av.size(); ++i) {
        Array.set(array, i, unpack(av.get(i), componentType, javaDeserializer));
      }
      return array;
    }));
    // Extension unpacker.
    unpackers.put(ValueType.EXTENSION, ((value, targetClass, javaDeserializer) -> {
      ExtensionValue ev = value.asExtensionValue();
      byte extType = ev.getType();
      if (extType == LANGUAGE_SPECIFIC_TYPE_EXTENSION_ID) {
        return javaDeserializer.deserialize(ev);
      }
      throw new IllegalArgumentException("Unknown extension type id " + ev.getType() + ".");
    }));
  }

  interface JavaSerializer {

    void serialize(Object object, MessagePacker packer) throws IOException;
  }

  interface JavaDeserializer {

    Object deserialize(ExtensionValue v);
  }

  interface TypePacker {

    void pack(Object object, MessagePacker packer,
        JavaSerializer javaSerializer) throws IOException;
  }

  interface TypeUnpacker {

    Object unpack(Value value, Class<?> targetClass,
        JavaDeserializer javaDeserializer);
  }

  private static boolean checkTypeCompatible(List<Class<?>> expected, Class<?> actual) {
    for (Class<?> expectedClass : expected) {
      if (actual.isAssignableFrom(expectedClass)) {
        return true;
      }
    }
    return false;
  }

  private static void pack(Object object, MessagePacker packer, JavaSerializer javaSerializer)
      throws IOException {
    TypePacker typePacker;
    if (object == null) {
      typePacker = NULL_PACKER;
    } else {
      Class<?> type = object.getClass();
      typePacker = packers.get(type);
      if (typePacker == null) {
        if (type.isArray()) {
          typePacker = ARRAY_PACKER;
        } else {
          typePacker = EXTENSION_PACKER;
        }
      }
    }
    typePacker.pack(object, packer, javaSerializer);
  }

  private static Object unpack(Value v, Class<?> type, JavaDeserializer javaDeserializer) {
    return unpackers.get(v.getValueType()).unpack(v, type, javaDeserializer);
  }

  public static Pair<byte[], Boolean> encode(Object obj) {
    MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
    try {
      // Reserve MESSAGE_PACK_OFFSET bytes for MessagePack bytes length.
      packer.writePayload(new byte[MESSAGE_PACK_OFFSET]);
      // Serialize input object by MessagePack.
      MutableBoolean isCrossLanguage = new MutableBoolean(true);
      pack(obj, packer, ((object, packer1) -> {
        byte[] payload = FstSerializer.encode(object);
        packer1.packExtensionTypeHeader(LANGUAGE_SPECIFIC_TYPE_EXTENSION_ID, payload.length);
        packer1.addPayload(payload);
        isCrossLanguage.setFalse();
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
      return ImmutablePair.of(msgpackBytes, isCrossLanguage.getValue());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  @SuppressWarnings("unchecked")
  public static <T> T decode(byte[] bs, Class<?> type) {
    try {
      // Read MessagePack bytes length.
      MessageUnpacker headerUnpacker = MessagePack.newDefaultUnpacker(bs, 0, MESSAGE_PACK_OFFSET);
      long msgpackBytesLength = headerUnpacker.unpackLong();
      headerUnpacker.close();
      // Check MessagePack bytes length is valid.
      Preconditions.checkState(MESSAGE_PACK_OFFSET + msgpackBytesLength <= bs.length);
      // Deserialize MessagePack bytes from MESSAGE_PACK_OFFSET.
      MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bs, MESSAGE_PACK_OFFSET,
          (int) msgpackBytesLength);
      Value v = unpacker.unpackValue();
      if (type == null) {
        type = Object.class;
      }
      return (T) unpack(v, type,
          ((ExtensionValue ev) -> FstSerializer.decode(ev.getData())));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
