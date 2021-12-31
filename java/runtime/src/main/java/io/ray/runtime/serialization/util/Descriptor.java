package io.ray.runtime.serialization.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.commons.lang3.StringUtils;

@SuppressWarnings("UnstableApiUsage")
public class Descriptor {
  private static final Map<Class<?>, SortedMap<Field, Descriptor>> descriptorCache =
      new HashMap<>();
  private static final Map<Class<?>, Map<String, List<Field>>> duplicateNameFieldsCache =
      new HashMap<>();

  private final Field field;
  private final Method readMethod;
  private final Method writeMethod;
  private final TypeToken<?> typeToken;

  public Descriptor(Field field, TypeToken<?> typeToken, Method readMethod, Method writeMethod) {
    this.field = field;
    this.readMethod = readMethod;
    this.writeMethod = writeMethod;
    this.typeToken = typeToken;
  }

  /**
   * Returns descriptors non-transient/non-static fields of class. If super class and sub class have
   * same field, use sub class field.
   */
  public static List<Descriptor> getDescriptors(Class<?> clz) {
    SortedMap<Field, Descriptor> allDescriptorsMap = getAllDescriptorsMap(clz);
    Map<String, List<Field>> duplicateNameFields = getDuplicateNameFields(clz);
    Preconditions.checkArgument(
        duplicateNameFields.size() == 0,
        String.format("%s has duplicate fields %s", clz, duplicateNameFields));
    return new ArrayList<>(allDescriptorsMap.values());
  }

  public static boolean hasDuplicateNameField(Class<?> clz) {
    return getDuplicateNameFields(clz).size() > 0;
  }

  public static synchronized Map<String, List<Field>> getDuplicateNameFields(Class<?> clz) {
    return duplicateNameFieldsCache.computeIfAbsent(
        clz,
        k -> {
          SortedMap<Field, Descriptor> allDescriptorsMap = getAllDescriptorsMap(clz);
          Map<String, List<Field>> duplicateNameFields = new HashMap<>();
          for (Field field : allDescriptorsMap.keySet()) {
            duplicateNameFields.compute(
                field.getName(),
                (fieldName, fields) -> {
                  if (fields == null) {
                    fields = new ArrayList<>();
                  }
                  fields.add(field);
                  return fields;
                });
          }
          duplicateNameFields =
              Maps.filterValues(
                  duplicateNameFields, fields -> Objects.requireNonNull(fields).size() > 1);
          return duplicateNameFields;
        });
  }

  /**
   * Return all non-transient/non-static fields of {@code clz} in a deterministic order with field
   * name first and declaring class second. Super class and sub class can have same name field.
   */
  public static Set<Field> getFields(Class<?> clz) {
    return getAllDescriptorsMap(clz).keySet();
  }

  /**
   * Returns descriptors map non-transient/non-static fields of class in a deterministic order with
   * field name first and declaring class second. Super class and sub class can have same name
   * field.
   */
  public static synchronized SortedMap<Field, Descriptor> getAllDescriptorsMap(Class<?> clz) {
    SortedMap<Field, Descriptor> map = descriptorCache.get(clz);
    if (map != null) {
      return map;
    }
    List<Field> fieldList = new ArrayList<>();
    Class<?> clazz = clz;
    Map<Tuple2<Class, String>, Method> methodMap = new HashMap<>();
    do {
      Field[] fields = clazz.getDeclaredFields();
      for (Field field : fields) {
        int modifiers = field.getModifiers();
        // final and non-private field validation left to {@link isBean(clz)}
        if (!Modifier.isTransient(modifiers) && !Modifier.isStatic(modifiers)) {
          fieldList.add(field);
        }
      }
      Arrays.stream(clazz.getDeclaredMethods())
          .filter(m -> !Modifier.isPrivate(m.getModifiers()))
          // if override, use subClass method; getter/setter method won't overload
          .forEach(m -> methodMap.put(Tuple2.of(m.getDeclaringClass(), m.getName()), m));
      clazz = clazz.getSuperclass();
    } while (clazz != null);

    for (Class<?> anInterface : clz.getInterfaces()) {
      Method[] methods = anInterface.getDeclaredMethods();
      for (Method method : methods) {
        if (method.isDefault()) {
          methodMap.put(Tuple2.of(method.getDeclaringClass(), method.getName()), method);
        }
      }
    }

    // use TreeMap to sort to fix field order
    TreeMap<Field, Descriptor> descriptorMap =
        new TreeMap<>(
            ((f1, f2) -> {
              int compare = f1.getName().compareTo(f2.getName());
              if (compare == 0) { // class and super classes have same name field
                return f1.getDeclaringClass().getName().compareTo(f2.getDeclaringClass().getName());
              } else {
                return compare;
              }
            }));
    for (Field field : fieldList) {
      Class<?> fieldDeclaringClass = field.getDeclaringClass();
      String fieldName = field.getName();
      String cap = StringUtils.capitalize(fieldName);
      Method getter;
      if ("boolean".equalsIgnoreCase(field.getType().getSimpleName())) {
        getter = methodMap.get(Tuple2.of(fieldDeclaringClass, "is" + cap));
      } else {
        getter = methodMap.get(Tuple2.of(fieldDeclaringClass, "get" + cap));
      }
      if (getter != null) {
        if (getter.getParameterCount() != 0
            || !getter
                .getGenericReturnType()
                .getTypeName()
                .equals(field.getGenericType().getTypeName())) {
          getter = null;
        }
      }
      Method setter = methodMap.get(Tuple2.of(fieldDeclaringClass, "set" + cap));
      if (setter != null) {
        if (setter.getParameterCount() != 1
            || !setter
                .getGenericParameterTypes()[0]
                .getTypeName()
                .equals(field.getGenericType().getTypeName())) {
          setter = null;
        }
      }
      TypeToken fieldType = TypeToken.of(field.getGenericType());
      descriptorMap.put(field, new Descriptor(field, fieldType, getter, setter));
    }
    descriptorCache.put(clz, descriptorMap);
    return descriptorMap;
  }

  public Field getField() {
    return field;
  }

  public String getName() {
    return field.getName();
  }

  public int getModifiers() {
    return field.getModifiers();
  }

  public Method getReadMethod() {
    return readMethod;
  }

  public Method getWriteMethod() {
    return writeMethod;
  }

  public TypeToken<?> getTypeToken() {
    return typeToken;
  }

  @Override
  public String toString() {
    return "Descriptor{"
        + "field="
        + field
        + ", readMethod="
        + readMethod
        + ", writeMethod="
        + writeMethod
        + ", typeToken="
        + typeToken
        + '}';
  }
}
