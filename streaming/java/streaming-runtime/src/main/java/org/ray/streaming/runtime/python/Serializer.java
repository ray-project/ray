package org.ray.streaming.runtime.python;

import com.google.common.collect.Maps;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class Serializer {
  private enum TypeSerializer {
    NIL((byte) 0) {
      @Override
      ByteBuffer serialize(Serializer serializer, ByteBuffer buffer, Object obj) {
        buffer = grow(buffer, 1);
        buffer.put(TypeSerializer.NIL.id);
        return buffer;
      }

      @Override
      Object deserialize(Serializer serializer, ByteBuffer buffer) {
        return null;
      }
    },
    BOOLEAN((byte) 1) {
      @Override
      ByteBuffer serialize(Serializer serializer, ByteBuffer buffer, Object obj) {
        buffer = grow(buffer, 2);
        buffer.put(BOOLEAN.id);
        if ((Boolean) obj) {
          buffer.put((byte) 1);
        } else {
          buffer.put((byte) 0);
        }
        return buffer;
      }

      @Override
      Object deserialize(Serializer serializer, ByteBuffer buffer) {
        if (buffer.get() == 0) {
          return Boolean.FALSE;
        } else {
          return Boolean.TRUE;
        }
      }
    },
    INTEGER((byte) 2) {
      @Override
      ByteBuffer serialize(Serializer serializer, ByteBuffer buffer, Object obj) {
        buffer = grow(buffer, 1 + 4);
        buffer.put(TypeSerializer.INTEGER.id);
        buffer.putInt((Integer) obj);
        return buffer;
      }

      @Override
      Object deserialize(Serializer serializer, ByteBuffer buffer) {
        return buffer.getInt();
      }
    },
    STRING((byte) 3) {
      @Override
      ByteBuffer serialize(Serializer serializer, ByteBuffer buffer, Object obj) {
        byte[] utf8Bytes = ((String) obj).getBytes(StandardCharsets.UTF_8);
        buffer = grow(buffer, 1 + 4 + utf8Bytes.length);
        buffer.put(TypeSerializer.STRING.id);
        buffer.putInt(utf8Bytes.length);
        buffer.put(utf8Bytes);
        return buffer;
      }

      @Override
      Object deserialize(Serializer serializer, ByteBuffer buffer) {
        int utf8Length = buffer.getInt();
        byte[] utf8Bytes = new byte[utf8Length];
        buffer.get(utf8Bytes);
        return new String(utf8Bytes, StandardCharsets.UTF_8);
      }
    },
    BINARY((byte) 4) {
      @Override
      ByteBuffer serialize(Serializer serializer, ByteBuffer buffer, Object obj) {
        byte[] bytes = (byte[]) obj;
        buffer = grow(buffer, 1 + 4 + bytes.length);
        buffer.put(TypeSerializer.BINARY.id);
        buffer.putInt(bytes.length);
        buffer.put(bytes);
        return buffer;
      }

      @Override
      Object deserialize(Serializer serializer, ByteBuffer buffer) {
        int length = buffer.getInt();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
      }
    },
    ARRAY((byte) 5) {
      @Override
      ByteBuffer serialize(Serializer serializer, ByteBuffer buffer, Object obj) {
        Collection collection = (Collection) obj;
        buffer = grow(buffer, 1 + 4);
        buffer.put(TypeSerializer.ARRAY.id);
        buffer.putInt(collection.size());
        for (Object o : collection) {
          serializer.serialize(buffer, o);
        }
        return buffer;
      }

      @Override
      Object deserialize(Serializer serializer, ByteBuffer buffer) {
        int size = buffer.getInt();
        List<Object> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
          list.add(serializer.deserialize(buffer));
        }
        return list;
      }
    },
    MAP((byte) 6) {
      @Override
      ByteBuffer serialize(Serializer serializer, ByteBuffer buffer, Object obj) {
        Map map = (Map) obj;
        buffer = grow(buffer, 1 + 4);
        buffer.put(TypeSerializer.MAP.id);
        buffer.putInt(map.size());
        for (Object o : map.entrySet()) {
          Map.Entry e = (Map.Entry) o;
          serializer.serialize(buffer, e.getKey());
          serializer.serialize(buffer, e.getValue());
        }
        return buffer;
      }

      @Override
      Object deserialize(Serializer serializer, ByteBuffer buffer) {
        int size = buffer.getInt();
        Map<Object, Object> map = new HashMap<>();
        for (int i = 0; i < size; i++) {
          map.put(serializer.deserialize(buffer), serializer.deserialize(buffer));
        }
        return map;
      }
    };

    private byte id;

    TypeSerializer(byte id) {
      this.id = id;
    }

    public byte id() {
      return id;
    }

    abstract ByteBuffer serialize(Serializer serializer, ByteBuffer buffer, Object obj);

    abstract Object deserialize(Serializer serializer, ByteBuffer buffer);
  }

  private static ByteOrder order = ByteOrder.LITTLE_ENDIAN;

  private Map<Class, TypeSerializer> classToTypeIdMap = Maps.newHashMap();
  private TypeSerializer[] typeIdToSerializer;

  {
    classToTypeIdMap.put(Boolean.class, TypeSerializer.BOOLEAN);
    classToTypeIdMap.put(Integer.class, TypeSerializer.INTEGER);
    classToTypeIdMap.put(String.class, TypeSerializer.STRING);
    classToTypeIdMap.put(byte[].class, TypeSerializer.BINARY);
    classToTypeIdMap.put(ArrayList.class, TypeSerializer.ARRAY);
    classToTypeIdMap.put(HashMap.class, TypeSerializer.MAP);

    byte maxTypeID = Arrays.stream(TypeSerializer.values())
        .max(Comparator.comparingInt(TypeSerializer::id)).get().id();
    typeIdToSerializer = new TypeSerializer[maxTypeID];
    Arrays.stream(TypeSerializer.values()).forEach(s -> {
      typeIdToSerializer[s.id()] = s;
    });
  }

  private TypeSerializer getSerializer(Class clz) {
    TypeSerializer typeSerializerId = classToTypeIdMap.get(clz);
    if (typeSerializerId == null) {
      if (Collection.class.isAssignableFrom(clz)) {
        typeSerializerId = TypeSerializer.ARRAY;
        classToTypeIdMap.put(clz, typeSerializerId);
      } else if (Map.class.isAssignableFrom(clz)) {
        typeSerializerId = TypeSerializer.MAP;
        classToTypeIdMap.put(clz, typeSerializerId);
      } else {
        throw new UnsupportedOperationException("Unsupported type " + clz);
      }
    }
    return typeSerializerId;
  }

  byte[] serialize(Object object) {
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.order(order);
    buffer = serialize(buffer, object);
    byte[] arr = new byte[buffer.remaining()];
    buffer.get(arr);
    return arr;
  }

  private ByteBuffer serialize(ByteBuffer buffer, Object object) {
    if (object == null) {
      return TypeSerializer.NIL.serialize(this, buffer, null);
    } else {
      getSerializer(object.getClass()).serialize(this, buffer, object);
      return buffer;
    }
  }

  Object deserialize(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    buffer.order(order);
    return deserialize(buffer);
  }

  Object deserialize(ByteBuffer buffer) {
    byte typeId = buffer.get();
    TypeSerializer typeSerializer = typeIdToSerializer[typeId];
    return typeSerializer.deserialize(this, buffer);
  }

  private static ByteBuffer grow(ByteBuffer buffer, int size) {
    if (buffer.remaining() < size) {
      int newSize = (buffer.position() + size) * 2;
      ByteBuffer newBuffer = ByteBuffer.allocate(newSize);
      buffer.flip();
      newBuffer.put(buffer);
      buffer.order(order);
      return newBuffer;
    } else {
      return buffer;
    }
  }

}
