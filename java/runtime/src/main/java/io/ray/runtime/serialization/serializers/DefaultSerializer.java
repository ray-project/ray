package io.ray.runtime.serialization.serializers;

import io.ray.runtime.io.MemoryBuffer;
import io.ray.runtime.io.Platform;
import io.ray.runtime.serialization.RaySerde;
import io.ray.runtime.serialization.Serializer;
import io.ray.runtime.serialization.resolver.ReferenceResolver;
import io.ray.runtime.serialization.util.Descriptor;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeSet;

/**
 * This is the default serializer when the object can't be serialized by other serializers such as:
 *
 * <ul>
 *   <li>non-public class
 *   <li>non-static class
 *   <li>inner class
 *   <li>local class
 *   <li>anonymous class
 *   <li>class that can't be handled by other serializers or codegen-based serializers
 * </ul>
 *
 * <p>Comparing to CodegenSerializer, this serializer allow parent class and child class to have
 * duplicate fields names.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class DefaultSerializer<T> extends Serializer<T> {
  private final ReferenceResolver referenceResolver;
  private Constructor<T> constructor;
  private final UnsafeFieldAccessor[] intFieldAccessors;
  private final UnsafeFieldAccessor[] longFieldAccessors;
  private final UnsafeFieldAccessor[] floatFieldAccessors;
  private final UnsafeFieldAccessor[] doubleFieldAccessors;
  private final boolean hasOtherPrimitiveFields;
  private final UnsafeFieldAccessor[] boolFieldAccessors;
  private final UnsafeFieldAccessor[] byteFieldAccessors;
  private final UnsafeFieldAccessor[] shortFieldAccessors;
  private final UnsafeFieldAccessor[] charFieldAccessors;
  // Use ObjectAccessor instead of FieldAccessor to avoid virtual methods calls.
  private final UnsafeFieldAccessor[] finalFieldAccessors;
  private final Serializer<?>[] finalFieldSerializers;
  private final UnsafeFieldAccessor[] otherFieldAccessors;
  private final int classVersionHash;

  public DefaultSerializer(RaySerde raySerDe, Class<T> cls) {
    super(raySerDe, cls);
    this.referenceResolver = raySerDe.getReferenceResolver();
    try {
      this.constructor = cls.getConstructor();
      if (!constructor.isAccessible()) {
        constructor.setAccessible(true);
      }
    } catch (Exception e) {
      constructor = null;
    }
    // all fields of class and super classes should be a consistent order between jvm process.
    SortedMap<Field, Descriptor> allFields = Descriptor.getAllDescriptorsMap(cls);
    classVersionHash = Serializers.computeVersionHash(cls);
    List<UnsafeFieldAccessor> intFieldAccessorsList = new ArrayList<>();
    List<UnsafeFieldAccessor> longFieldAccessorsList = new ArrayList<>();
    List<UnsafeFieldAccessor> floatFieldAccessorsList = new ArrayList<>();
    List<UnsafeFieldAccessor> doubleFieldAccessorsList = new ArrayList<>();
    List<UnsafeFieldAccessor> boolFieldAccessorsList = new ArrayList<>();
    List<UnsafeFieldAccessor> byteFieldAccessorsList = new ArrayList<>();
    List<UnsafeFieldAccessor> shortFieldAccessorsList = new ArrayList<>();
    List<UnsafeFieldAccessor> charFieldAccessorsList = new ArrayList<>();
    new ArrayList<>();
    Comparator<UnsafeFieldAccessor> comparator =
        (a1, a2) -> {
          // sort by type so that we can hit class info cache more possibly.
          // sort by field name to fix order if type is same.
          int c = a2.getField().getType().getName().compareTo(a1.getField().getType().getName());
          if (c == 0) {
            return a2.getField().getName().compareTo(a1.getField().getName());
          } else {
            return c;
          }
        };
    TreeSet<UnsafeFieldAccessor> finalFieldAccessorsList = new TreeSet<>(comparator);
    TreeSet<UnsafeFieldAccessor> otherFieldAccessorsList = new TreeSet<>(comparator);
    for (Map.Entry<Field, Descriptor> descriptorEntry : allFields.entrySet()) {
      Field field = descriptorEntry.getKey();
      Class<?> fieldType = field.getType();
      UnsafeFieldAccessor unsafeFieldAccessor = new UnsafeFieldAccessor(field);
      if (fieldType == int.class) {
        intFieldAccessorsList.add(unsafeFieldAccessor);
      } else if (fieldType == long.class) {
        longFieldAccessorsList.add(unsafeFieldAccessor);
      } else if (fieldType == float.class) {
        floatFieldAccessorsList.add(unsafeFieldAccessor);
      } else if (fieldType == double.class) {
        doubleFieldAccessorsList.add(unsafeFieldAccessor);
      } else if (fieldType == boolean.class) {
        boolFieldAccessorsList.add(unsafeFieldAccessor);
      } else if (fieldType == byte.class) {
        byteFieldAccessorsList.add(unsafeFieldAccessor);
      } else if (fieldType == short.class) {
        shortFieldAccessorsList.add(unsafeFieldAccessor);
      } else if (fieldType == char.class) {
        charFieldAccessorsList.add(unsafeFieldAccessor);
      } else if (Modifier.isFinal(fieldType.getModifiers()) && fieldType != cls) {
        // avoid recursive
        finalFieldAccessorsList.add(unsafeFieldAccessor);
      } else {
        otherFieldAccessorsList.add(unsafeFieldAccessor);
      }
    }
    this.intFieldAccessors = intFieldAccessorsList.toArray(new UnsafeFieldAccessor[0]);
    this.longFieldAccessors = longFieldAccessorsList.toArray(new UnsafeFieldAccessor[0]);
    this.floatFieldAccessors = floatFieldAccessorsList.toArray(new UnsafeFieldAccessor[0]);
    this.doubleFieldAccessors = doubleFieldAccessorsList.toArray(new UnsafeFieldAccessor[0]);
    this.boolFieldAccessors = boolFieldAccessorsList.toArray(new UnsafeFieldAccessor[0]);
    this.byteFieldAccessors = byteFieldAccessorsList.toArray(new UnsafeFieldAccessor[0]);
    this.shortFieldAccessors = shortFieldAccessorsList.toArray(new UnsafeFieldAccessor[0]);
    this.charFieldAccessors = charFieldAccessorsList.toArray(new UnsafeFieldAccessor[0]);
    this.hasOtherPrimitiveFields =
        byteFieldAccessors.length > 0
            || shortFieldAccessors.length > 0
            || charFieldAccessors.length > 0;
    this.finalFieldAccessors = finalFieldAccessorsList.toArray(new UnsafeFieldAccessor[0]);
    this.finalFieldSerializers =
        finalFieldAccessorsList.stream()
            .map(
                accessor ->
                    raySerDe.getClassResolver().getSerializer(accessor.getField().getType()))
            .toArray(Serializer[]::new);
    this.otherFieldAccessors = otherFieldAccessorsList.toArray(new UnsafeFieldAccessor[0]);
  }

  @Override
  public void write(MemoryBuffer buffer, T value) {
    if (raySerde.checkClassVersion()) {
      buffer.writeInt(classVersionHash);
    }
    writePrimitives(buffer, value);
    UnsafeFieldAccessor[] finalFieldAccessors = this.finalFieldAccessors;
    Serializer<?>[] finalFieldSerializers = this.finalFieldSerializers;
    for (int i = 0; i < finalFieldAccessors.length; i++) {
      UnsafeFieldAccessor fieldAccessor = finalFieldAccessors[i];
      Object fieldValue = fieldAccessor.getObject(value);
      if (!referenceResolver.writeReferenceOrNull(buffer, fieldValue)) {
        // don't use fieldValue.getClass(), because fieldAccessor.field.getType() may be
        // Object.class
        // while fieldValue.getClass() is String.class, which can't be known ahead for read method.
        Class<?> fieldClass = fieldAccessor.getField().getType();
        // fast path for frequent types.
        if (fieldClass == Long.class) {
          buffer.writeLong((Long) fieldValue);
        } else if (fieldClass == Integer.class) {
          buffer.writeInt((Integer) fieldValue);
        } else if (fieldClass == Double.class) {
          buffer.writeDouble((Double) fieldValue);
        } else {
          Serializer serializer = finalFieldSerializers[i];
          serializer.write(buffer, fieldValue);
        }
      }
    }
    for (UnsafeFieldAccessor fieldAccessor : this.otherFieldAccessors) {
      Object fieldValue = fieldAccessor.getObject(value);
      if (!referenceResolver.writeReferenceOrNull(buffer, fieldValue)) {
        raySerde.serializeNonReferenceToJava(buffer, fieldValue);
      }
    }
  }

  private void writePrimitives(MemoryBuffer buffer, T value) {
    // fast path for frequent type
    for (UnsafeFieldAccessor fieldAccessor : intFieldAccessors) {
      buffer.writeInt(fieldAccessor.getInt(value));
    }
    for (UnsafeFieldAccessor fieldAccessor : longFieldAccessors) {
      buffer.writeLong(fieldAccessor.getLong(value));
    }
    for (UnsafeFieldAccessor fieldAccessor : floatFieldAccessors) {
      buffer.writeFloat(fieldAccessor.getFloat(value));
    }
    for (UnsafeFieldAccessor fieldAccessor : doubleFieldAccessors) {
      buffer.writeDouble(fieldAccessor.getDouble(value));
    }
    for (UnsafeFieldAccessor fieldAccessor : boolFieldAccessors) {
      buffer.writeBoolean(fieldAccessor.getBoolean(value));
    }
    if (hasOtherPrimitiveFields) {
      for (UnsafeFieldAccessor fieldAccessor : byteFieldAccessors) {
        buffer.writeByte(fieldAccessor.getByte(value));
      }
      for (UnsafeFieldAccessor fieldAccessor : shortFieldAccessors) {
        buffer.writeShort(fieldAccessor.getShort(value));
      }
      for (UnsafeFieldAccessor fieldAccessor : charFieldAccessors) {
        buffer.writeChar(fieldAccessor.getChar(value));
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public T read(MemoryBuffer buffer) {
    if (raySerde.checkClassVersion()) {
      int hash = buffer.readInt();
      Serializers.checkClassVersion(raySerde, hash, classVersionHash);
    }
    Object bean = newBean();
    referenceResolver.reference(bean);
    readPrimitives(bean, buffer);
    UnsafeFieldAccessor[] finalFieldAccessors = this.finalFieldAccessors;
    Serializer<?>[] finalFieldSerializers = this.finalFieldSerializers;
    for (int i = 0; i < finalFieldAccessors.length; i++) {
      UnsafeFieldAccessor fieldAccessor = finalFieldAccessors[i];
      Object fieldValue;
      // It's not a reference, we need read field data.
      if (referenceResolver.readReferenceOrNull(buffer) == RaySerde.NOT_NULL) {
        int nextReadRefId = referenceResolver.preserveReferenceId();
        Class<?> fieldClass = fieldAccessor.getField().getType();
        // fast path for frequent type
        if (fieldClass == Long.class) {
          fieldValue = buffer.readLong();
        } else if (fieldClass == Integer.class) {
          fieldValue = buffer.readInt();
        } else if (fieldClass == Double.class) {
          fieldValue = buffer.readDouble();
        } else {
          Serializer serializer = finalFieldSerializers[i];
          fieldValue = serializer.read(buffer);
        }
        raySerde.getReferenceResolver().setReadObject(nextReadRefId, fieldValue);
        fieldAccessor.putObject(bean, fieldValue);
      } else {
        fieldValue = referenceResolver.getReadObject();
        if (fieldValue != null) {
          fieldAccessor.putObject(bean, fieldValue);
        }
      }
    }
    for (UnsafeFieldAccessor fieldAccessor : this.otherFieldAccessors) {
      Object fieldValue;
      // It's not a reference, we need read field data.
      if (referenceResolver.readReferenceOrNull(buffer) == RaySerde.NOT_NULL) {
        int nextReadRefId = referenceResolver.preserveReferenceId();
        fieldValue = raySerde.deserializeNonReferenceFromJava(buffer);
        raySerde.getReferenceResolver().setReadObject(nextReadRefId, fieldValue);
        fieldAccessor.putObject(bean, fieldValue);
      } else {
        fieldValue = referenceResolver.getReadObject();
        if (fieldValue != null) {
          fieldAccessor.putObject(bean, fieldValue);
        }
      }
    }
    return (T) bean;
  }

  private void readPrimitives(Object bean, MemoryBuffer buffer) {
    // fast path for frequent type
    for (UnsafeFieldAccessor fieldAccessor : intFieldAccessors) {
      fieldAccessor.putInt(bean, buffer.readInt());
    }
    for (UnsafeFieldAccessor fieldAccessor : longFieldAccessors) {
      fieldAccessor.putLong(bean, buffer.readLong());
    }
    for (UnsafeFieldAccessor fieldAccessor : floatFieldAccessors) {
      fieldAccessor.putFloat(bean, buffer.readFloat());
    }
    for (UnsafeFieldAccessor fieldAccessor : doubleFieldAccessors) {
      fieldAccessor.putDouble(bean, buffer.readDouble());
    }
    for (UnsafeFieldAccessor fieldAccessor : boolFieldAccessors) {
      fieldAccessor.putBoolean(bean, buffer.readBoolean());
    }
    if (hasOtherPrimitiveFields) {
      for (UnsafeFieldAccessor fieldAccessor : byteFieldAccessors) {
        fieldAccessor.putByte(bean, buffer.readByte());
      }
      for (UnsafeFieldAccessor fieldAccessor : shortFieldAccessors) {
        fieldAccessor.putShort(bean, buffer.readShort());
      }
      for (UnsafeFieldAccessor fieldAccessor : charFieldAccessors) {
        fieldAccessor.putChar(bean, buffer.readChar());
      }
    }
  }

  private Object newBean() {
    if (constructor != null) {
      try {
        return constructor.newInstance();
      } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
        Platform.throwException(e);
      }
    }
    return Platform.newInstance(cls);
  }
}
