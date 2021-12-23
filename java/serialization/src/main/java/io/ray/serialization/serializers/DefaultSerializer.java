package io.ray.serialization.serializers;

import com.google.common.primitives.Primitives;
import io.ray.serialization.Fury;
import io.ray.serialization.FuryException;
import io.ray.serialization.resolver.ReferenceResolver;
import io.ray.serialization.util.CommonUtil;
import io.ray.serialization.util.Descriptor;
import io.ray.serialization.util.MemoryBuffer;
import io.ray.serialization.util.Platform;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

/**
 * This serializer is used only for java serialization:
 *
 * <ul>
 *   <li>non-public class
 *   <li>non-static class
 *   <li>lambda
 *   <li>inner class
 *   <li>local class
 *   <li>anonymous class
 *   <li>class that can't be handled by other serializers or codegen-based serializers
 * </ul>
 *
 * <p>Comparing to CodegenSerializer, this serializer allow parent class and child class to
 * have duplicate fields names.
 */
public final class DefaultSerializer<T> extends Serializer<T> {
  private final ReferenceResolver referenceResolver;
  private Constructor<T> constructor;
  private final Field[] fields;
  private UnsafeFieldAccessor[] intFieldAccessors;
  private UnsafeFieldAccessor[] longFieldAccessors;
  private UnsafeFieldAccessor[] floatFieldAccessors;
  private UnsafeFieldAccessor[] doubleFieldAccessors;
  private FieldAccessor[] finalFieldAccessors;
  private Serializer[] finalFieldSerializers;
  private FieldAccessor[] otherFieldAccessors;
  private final int classVersionHash;

  public DefaultSerializer(Fury fury, Class<T> cls) {
    super(fury, cls);
    this.referenceResolver = fury.getReferenceResolver();
    try {
      this.constructor = cls.getConstructor();
      if (!constructor.isAccessible()) {
        constructor.setAccessible(true);
      }
    } catch (Exception e) {
      CommonUtil.ignore(e);
    }
    // all fields of class and super classes should be a consistent order between jvm process.
    this.fields = Descriptor.getFields(cls).toArray(new Field[0]);
    buildFieldAccessors();
    classVersionHash = Serializers.computeVersionHash(cls);
  }

  private void buildFieldAccessors() {
    List<UnsafeFieldAccessor> intFieldAccessorsList = new ArrayList<>();
    List<UnsafeFieldAccessor> longFieldAccessorsList = new ArrayList<>();
    List<UnsafeFieldAccessor> floatFieldAccessorsList = new ArrayList<>();
    List<UnsafeFieldAccessor> doubleFieldAccessorsList = new ArrayList<>();
    Comparator<FieldAccessor> comparator =
        (a1, a2) -> {
          // sort by type so that we can hit class info cache more possibly.
          // sort by field name to fix order if type is same.
          int c = a2.field.getType().getName().compareTo(a1.field.getType().getName());
          if (c == 0) {
            return a2.field.getName().compareTo(a1.field.getName());
          } else {
            return c;
          }
        };
    TreeSet<FieldAccessor> finalFieldAccessorsList = new TreeSet<>(comparator);
    TreeSet<FieldAccessor> otherFieldAccessorsList = new TreeSet<>(comparator);
    for (Field field : fields) {
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
      } else if (Modifier.isFinal(fieldType.getModifiers()) && fieldType != cls) {
        finalFieldAccessorsList.add(FieldAccessor.createAccessor(field));
      } else {
        otherFieldAccessorsList.add(FieldAccessor.createAccessor(field));
      }
    }
    this.intFieldAccessors = intFieldAccessorsList.toArray(new UnsafeFieldAccessor[0]);
    this.longFieldAccessors = longFieldAccessorsList.toArray(new UnsafeFieldAccessor[0]);
    this.floatFieldAccessors = floatFieldAccessorsList.toArray(new UnsafeFieldAccessor[0]);
    this.doubleFieldAccessors = doubleFieldAccessorsList.toArray(new UnsafeFieldAccessor[0]);
    this.finalFieldAccessors = finalFieldAccessorsList.toArray(new FieldAccessor[0]);
    this.finalFieldSerializers =
        finalFieldAccessorsList.stream()
            .map(
                accessor ->
                    fury.getClassResolver()
                        .getSerializer(Primitives.wrap(accessor.field.getType())))
            .toArray(Serializer[]::new);
    this.otherFieldAccessors = otherFieldAccessorsList.toArray(new FieldAccessor[0]);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void write(Fury fury, MemoryBuffer buffer, T value) {
    if (fury.checkClassVersion()) {
      buffer.writeInt(classVersionHash);
    }
    writePrimitives(buffer, value);
    FieldAccessor[] finalFieldAccessors = this.finalFieldAccessors;
    Serializer[] finalFieldSerializers = this.finalFieldSerializers;
    for (int i = 0; i < finalFieldAccessors.length; i++) {
      FieldAccessor fieldAccessor = finalFieldAccessors[i];
      Object fieldValue = fieldAccessor.get(value);
      if (!referenceResolver.writeReferenceOrNull(buffer, fieldValue)) {
        // don't use fieldValue.getClass(), because fieldAccessor.field.getType() may be
        // Object.class
        // while fieldValue.getClass() is String.class, which can't be known ahead for read method.
        Class<?> fieldClass = fieldAccessor.field.getType();
        // fast path for frequent types
        if (fieldClass == Long.class) {
          buffer.writeLong((Long) fieldValue);
        } else if (fieldClass == Integer.class) {
          buffer.writeInt((Integer) fieldValue);
        } else if (fieldClass == Double.class) {
          buffer.writeDouble((Double) fieldValue);
        } else {
          Serializer serializer = finalFieldSerializers[i];
          serializer.write(fury, buffer, fieldValue);
        }
      }
    }
    for (FieldAccessor fieldAccessor : this.otherFieldAccessors) {
      Object fieldValue = fieldAccessor.get(value);
      if (!referenceResolver.writeReferenceOrNull(buffer, fieldValue)) {
        // don't use fieldValue.getClass(), because fieldAccessor.field.getType() may be
        // Object.class
        // while fieldValue.getClass() is String.class, which can't be known ahead for read method.
        fury.serializeNonReferenceToJava(buffer, fieldValue);
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
  }

  @SuppressWarnings("unchecked")
  @Override
  public T read(Fury fury, MemoryBuffer buffer, Class<T> type) {
    if (fury.checkClassVersion()) {
      int hash = buffer.readInt();
      Serializers.checkClassVersion(fury, hash, classVersionHash);
    }
    Object bean = newBean();
    referenceResolver.reference(bean);
    readPrimitives(bean, buffer);

    FieldAccessor[] finalFieldAccessors = this.finalFieldAccessors;
    Serializer[] finalFieldSerializers = this.finalFieldSerializers;
    for (int i = 0; i < finalFieldAccessors.length; i++) {
      FieldAccessor fieldAccessor = finalFieldAccessors[i];
      Object fieldValue;
      // It's not a reference, we need read field data.
      if (referenceResolver.readReferenceOrNull(buffer) == Fury.NOT_NULL) {
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
          fieldValue = serializer.read(fury, buffer, fieldClass);
        }
        if (fury.isReferenceTracking()) {
          fury.getReferenceResolver().setReadObject(nextReadRefId, fieldValue);
        }
        fieldAccessor.set(bean, fieldValue);
      } else {
        if (fury.isReferenceTracking()) {
          fieldValue = referenceResolver.getReadObject();
          if (fieldValue != null) {
            fieldAccessor.set(bean, fieldValue);
          }
        }
      }
    }

    for (FieldAccessor fieldAccessor : this.otherFieldAccessors) {
      Object fieldValue;
      // It's not a reference, we need read field data.
      if (referenceResolver.readReferenceOrNull(buffer) == Fury.NOT_NULL) {
        int nextReadRefId = referenceResolver.preserveReferenceId();
        fieldValue = fury.deserializeNonReferenceFromJava(buffer);
        if (fury.isReferenceTracking()) {
          fury.getReferenceResolver().setReadObject(nextReadRefId, fieldValue);
        }
        fieldAccessor.set(bean, fieldValue);
      } else {
        if (fury.isReferenceTracking()) {
          fieldValue = referenceResolver.getReadObject();
          if (fieldValue != null) {
            fieldAccessor.set(bean, fieldValue);
          }
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
