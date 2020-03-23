package org.ray.runtime.serializer;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.math.BigInteger;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.ExtensionValue;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.Value;

// We can't pack List / Map by MessagePack, because we don't know the type class when unpacking.
public class MessagePackSerializer {

  private static final byte CROSS_LANGUAGE_TYPE_EXTENSION_ID = 100;
  private static final byte LANGUAGE_SPECIFIC_TYPE_EXTENSION_ID = 101;
  // MessagePack length is an int takes up to 9 bytes.
  // https://github.com/msgpack/msgpack/blob/master/spec.md#int-format-family
  private static final int MESSAGE_PACK_OFFSET = 9;

  interface JavaSerializer {

    void serialize(Object object, MessagePacker packer) throws IOException;
  }

  interface JavaDeserializer {

    Object deserialize(ExtensionValue v);
  }


  private static void pack(Object object, MessagePacker packer, JavaSerializer javaSerializer)
      throws IOException {
    if (object == null) {
      packer.packNil();
    } else if (object instanceof Byte) {
      packer.packByte((Byte) object);
    } else if (object instanceof Boolean) {
      packer.packBoolean((Boolean) object);
    } else if (object instanceof Double) {
      packer.packDouble((Double) object);
    } else if (object instanceof Float) {
      packer.packFloat((Float) object);
    } else if (object instanceof Integer) {
      packer.packInt((Integer) object);
    } else if (object instanceof Long) {
      packer.packLong((Long) object);
    } else if (object instanceof Short) {
      packer.packShort((Short) object);
    } else if (object instanceof BigInteger) {
      packer.packBigInteger((BigInteger) object);
    } else if (object instanceof String) {
      packer.packString((String) object);
    } else if (object instanceof byte[]) {
      byte[] bytes = (byte[]) object;
      packer.packBinaryHeader(bytes.length);
      packer.writePayload(bytes);
    } else if (object.getClass().isArray()) {
      int length = Array.getLength(object);
      packer.packArrayHeader(length);
      for (int i = 0; i < length; ++i) {
        pack(Array.get(object, i), packer, javaSerializer);
      }
    } else {
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
        pack(data, crossTypePacker, javaSerializer);
        // Put the serialized bytes to CROSS_LANGUAGE_TYPE_EXTENSION_ID.
        byte[] payload = crossTypePacker.toByteArray();
        packer.packExtensionTypeHeader(CROSS_LANGUAGE_TYPE_EXTENSION_ID, payload.length);
        packer.addPayload(payload);
      } catch (Exception e) {
        javaSerializer.serialize(object, packer);
      }
    }
  }

  private static Object unpack(Value v, Class<?> type, JavaDeserializer javaDeserializer) {
    switch (v.getValueType()) {
      case NIL:
        return null;
      case BOOLEAN:
        if (type.isAssignableFrom(Boolean.class) || type.isAssignableFrom(boolean.class)) {
          return v.asBooleanValue().getBoolean();
        } else {
          throw new IllegalArgumentException("expected " + type + ", actual Boolean!");
        }
      case INTEGER:
        IntegerValue iv = v.asIntegerValue();
        if (iv.isInByteRange() &&
            (type.isAssignableFrom(Byte.class) || type.isAssignableFrom(byte.class))) {
          return iv.asByte();
        } else if (iv.isInShortRange() &&
            (type.isAssignableFrom(Short.class) || type.isAssignableFrom(short.class))) {
          return iv.asShort();
        } else if (iv.isInIntRange() &&
            (type.isAssignableFrom(Integer.class) || type.isAssignableFrom(int.class))) {
          return iv.asInt();
        } else if (iv.isInLongRange() &&
            (type.isAssignableFrom(Long.class) || type.isAssignableFrom(long.class))) {
          return iv.asLong();
        } else if (type.isAssignableFrom(BigInteger.class)) {
          return iv.asBigInteger();
        } else {
          throw new IllegalArgumentException("expected " + type + ", actual Integer!");
        }
      case FLOAT:
        if (type.isAssignableFrom(Double.class) || type.isAssignableFrom(double.class)) {
          return v.asFloatValue().toDouble(); // use as double
        } else if (type.isAssignableFrom(Float.class) || type.isAssignableFrom(float.class)) {
          return v.asFloatValue().toFloat();
        } else {
          throw new IllegalArgumentException("expected " + type + ", actual Float!");
        }
      case STRING:
        if (type.isAssignableFrom(String.class)) {
          return v.asStringValue().asString();
        } else {
          throw new IllegalArgumentException("expected " + type + ", actual String!");
        }
      case BINARY:
        if (type.isAssignableFrom(byte[].class)) {
          return v.asBinaryValue().asByteArray();
        } else {
          throw new IllegalArgumentException("expected " + type + ", actual byte[]!");
        }
      case ARRAY:
        if (type.isArray() || type.isAssignableFrom(Object.class)) {
          ArrayValue a = v.asArrayValue();
          Class<?> componentType = type.isArray() ? type.getComponentType() : Object.class;
          Object array = Array.newInstance(componentType, a.size());
          for (int i = 0; i < a.size(); ++i) {
            Value value = a.get(i);
            Array.set(array, i, unpack(value, componentType, javaDeserializer));
          }
          return array;
        } else {
          throw new IllegalArgumentException("expected " + type + ", actual Array!");
        }
      case EXTENSION:
        ExtensionValue ev = v.asExtensionValue();
        byte extType = ev.getType();
        if (extType == CROSS_LANGUAGE_TYPE_EXTENSION_ID) {
          try {
            MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(ev.getData());
            Value crossValue = unpacker.unpackValue();
            // data is [type id, cross data object]
            Object[] data = (Object[]) unpack(crossValue, Object[].class, javaDeserializer);
            Preconditions.checkState(data != null && data.length == 2);
            Integer crossTypeId = ((Number) data[0]).intValue();
            Object[] crossData = (Object[]) data[1];
            // Get type by type id and call KEY_FROM_CROSS_DATA method with cross data on it.
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
        } else {
          throw new IllegalArgumentException("expected " + type + ", actual extension type " +
              extType);
        }
      default:
        throw new IllegalArgumentException(
            "expected " + type + ", actual type " + v.getValueType());
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
        byte[] payload = FSTSerializer.encode(object, javaEncoderMeta, classLoader);
        packer1.packExtensionTypeHeader(LANGUAGE_SPECIFIC_TYPE_EXTENSION_ID, payload.length);
        packer1.addPayload(payload);
        meta.isCrossLanguage = false;
      }));
      byte[] msgpackBytes = packer.toByteArray();
      // Serialize MessagePack bytes length.
      MessageBufferPacker headerPacker = MessagePack.newDefaultBufferPacker();
      headerPacker.packLong(msgpackBytes.length - MESSAGE_PACK_OFFSET);
      byte[] msgpackBytesLength = headerPacker.toByteArray();
      // Check serialized MessagePack bytes length is valid.
      Preconditions.checkState(msgpackBytesLength.length <= MESSAGE_PACK_OFFSET);
      // Write MessagePack bytes length to reserved buffer at the beginning.
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
      // Handle null has not class.
      if (type == null) {
        type = Object.class;
      }
      return (T) unpack(v, type, ((ExtensionValue ev) -> FSTSerializer.decode(ev.getData(),
          classLoader)));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void setClassloader(ClassLoader classLoader) {
    FSTSerializer.setClassloader(classLoader);
  }
}
