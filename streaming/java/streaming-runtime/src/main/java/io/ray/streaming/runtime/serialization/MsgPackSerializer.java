package io.ray.streaming.runtime.serialization;

import com.google.common.io.BaseEncoding;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.FloatValue;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;

public class MsgPackSerializer {

  public byte[] serialize(Object obj) {
    MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
    serialize(obj, packer);
    return packer.toByteArray();
  }

  private void serialize(Object obj, MessageBufferPacker packer) {
    try {
      if (obj == null) {
        packer.packNil();
      } else {
        Class<?> clz = obj.getClass();
        if (clz == Boolean.class) {
          packer.packBoolean((Boolean) obj);
        } else if (clz == Byte.class) {
          packer.packByte((Byte) obj);
        } else if (clz == Short.class) {
          packer.packShort((Short) obj);
        } else if (clz == Integer.class) {
          packer.packInt((Integer) obj);
        } else if (clz == Long.class) {
          packer.packLong((Long) obj);
        } else if (clz == Double.class) {
          packer.packDouble((Double) obj);
        } else if (clz == byte[].class) {
          byte[] bytes = (byte[]) obj;
          packer.packBinaryHeader(bytes.length);
          packer.writePayload(bytes);
        } else if (clz == String.class) {
          packer.packString((String) obj);
        } else if (obj instanceof Collection) {
          Collection collection = (Collection) (obj);
          packer.packArrayHeader(collection.size());
          for (Object o : collection) {
            serialize(o, packer);
          }
        } else if (obj instanceof Map) {
          Map map = (Map) (obj);
          packer.packMapHeader(map.size());
          for (Object o : map.entrySet()) {
            Map.Entry e = (Map.Entry) o;
            serialize(e.getKey(), packer);
            serialize(e.getValue(), packer);
          }
        } else {
          throw new UnsupportedOperationException("Unsupported type " + clz);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Serialize error for object " + obj, e);
    }
  }

  public Object deserialize(byte[] bytes) {
    try {
      MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bytes);
      return deserialize(unpacker.unpackValue());
    } catch (Exception e) {
      String hex = BaseEncoding.base16().lowerCase().encode(bytes);
      throw new RuntimeException("Deserialize error: " + hex, e);
    }
  }

  private Object deserialize(Value value) {
    switch (value.getValueType()) {
      case NIL:
        return null;
      case BOOLEAN:
        return value.asBooleanValue().getBoolean();
      case INTEGER:
        IntegerValue iv = value.asIntegerValue();
        if (iv.isInByteRange()) {
          return iv.toByte();
        } else if (iv.isInShortRange()) {
          return iv.toShort();
        } else if (iv.isInIntRange()) {
          return iv.toInt();
        } else if (iv.isInLongRange()) {
          return iv.toLong();
        } else {
          return iv.toBigInteger();
        }
      case FLOAT:
        FloatValue fv = value.asFloatValue();
        return fv.toDouble();
      case STRING:
        return value.asStringValue().asString();
      case BINARY:
        return value.asBinaryValue().asByteArray();
      case ARRAY:
        ArrayValue arrayValue = value.asArrayValue();
        List<Object> list = new ArrayList<>(arrayValue.size());
        for (Value elem : arrayValue) {
          list.add(deserialize(elem));
        }
        return list;
      case MAP:
        MapValue mapValue = value.asMapValue();
        Map<Object, Object> map = new HashMap<>();
        for (Map.Entry<Value, Value> entry : mapValue.entrySet()) {
          map.put(deserialize(entry.getKey()), deserialize(entry.getValue()));
        }
        return map;
      default:
        throw new UnsupportedOperationException("Unsupported type " + value.getValueType());
    }
  }
}
