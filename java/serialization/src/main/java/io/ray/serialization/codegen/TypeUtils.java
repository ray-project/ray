package io.ray.serialization.codegen;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import io.ray.serialization.util.Tuple2;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("UnstableApiUsage")
public class TypeUtils {
  public static final String JAVA_BOOLEAN = "boolean";
  public static final String JAVA_BYTE = "byte";
  public static final String JAVA_SHORT = "short";
  public static final String JAVA_INT = "int";
  public static final String JAVA_LONG = "long";
  public static final String JAVA_FLOAT = "float";
  public static final String JAVA_DOUBLE = "double";

  public static final TypeToken<?> PRIMITIVE_VOID_TYPE = TypeToken.of(void.class);
  public static final TypeToken<?> VOID_TYPE = TypeToken.of(Void.class);

  public static final TypeToken<?> PRIMITIVE_BYTE_TYPE = TypeToken.of(byte.class);
  public static final TypeToken<?> PRIMITIVE_BOOLEAN_TYPE = TypeToken.of(boolean.class);
  public static final TypeToken<?> PRIMITIVE_CHAR_TYPE = TypeToken.of(char.class);
  public static final TypeToken<?> PRIMITIVE_SHORT_TYPE = TypeToken.of(short.class);
  public static final TypeToken<?> PRIMITIVE_INT_TYPE = TypeToken.of(int.class);
  public static final TypeToken<?> PRIMITIVE_LONG_TYPE = TypeToken.of(long.class);
  public static final TypeToken<?> PRIMITIVE_FLOAT_TYPE = TypeToken.of(float.class);
  public static final TypeToken<?> PRIMITIVE_DOUBLE_TYPE = TypeToken.of(double.class);

  public static final TypeToken<?> BYTE_TYPE = TypeToken.of(Byte.class);
  public static final TypeToken<?> BOOLEAN_TYPE = TypeToken.of(Boolean.class);
  public static final TypeToken<?> CHAR_TYPE = TypeToken.of(Character.class);
  public static final TypeToken<?> SHORT_TYPE = TypeToken.of(Short.class);
  public static final TypeToken<?> INT_TYPE = TypeToken.of(Integer.class);
  public static final TypeToken<?> LONG_TYPE = TypeToken.of(Long.class);
  public static final TypeToken<?> FLOAT_TYPE = TypeToken.of(Float.class);
  public static final TypeToken<?> DOUBLE_TYPE = TypeToken.of(Double.class);

  public static final TypeToken<?> STRING_TYPE = TypeToken.of(String.class);
  public static final TypeToken<?> BIG_DECIMAL_TYPE = TypeToken.of(BigDecimal.class);
  public static final TypeToken<?> BIG_INTEGER_TYPE = TypeToken.of(BigInteger.class);
  public static final TypeToken<?> DATE_TYPE = TypeToken.of(Date.class);
  public static final TypeToken<?> LOCAL_DATE_TYPE = TypeToken.of(LocalDate.class);
  public static final TypeToken<?> TIMESTAMP_TYPE = TypeToken.of(Timestamp.class);
  public static final TypeToken<?> INSTANT_TYPE = TypeToken.of(Instant.class);
  public static final TypeToken<?> BINARY_TYPE = TypeToken.of(byte[].class);

  public static final TypeToken<?> ITERABLE_TYPE = TypeToken.of(Iterable.class);
  public static final TypeToken<?> COLLECTION_TYPE = TypeToken.of(Collection.class);
  public static final TypeToken<?> LIST_TYPE = TypeToken.of(List.class);
  public static final TypeToken<?> SET_TYPE = TypeToken.of(Set.class);
  public static final TypeToken<?> MAP_TYPE = TypeToken.of(Map.class);

  public static final TypeToken<?> OBJECT_TYPE = TypeToken.of(Object.class);

  public static Type ITERATOR_RETURN_TYPE;
  public static Type NEXT_RETURN_TYPE;
  public static Type KEY_SET_RETURN_TYPE;
  public static Type VALUES_RETURN_TYPE;

  public static final TypeToken<?> PRIMITIVE_BYTE_ARRAY_TYPE = TypeToken.of(byte[].class);
  public static final TypeToken<?> PRIMITIVE_BOOLEAN_ARRAY_TYPE = TypeToken.of(boolean[].class);
  public static final TypeToken<?> PRIMITIVE_SHORT_ARRAY_TYPE = TypeToken.of(short[].class);
  public static final TypeToken<?> PRIMITIVE_INT_ARRAY_TYPE = TypeToken.of(int[].class);
  public static final TypeToken<?> PRIMITIVE_LONG_ARRAY_TYPE = TypeToken.of(long[].class);
  public static final TypeToken<?> PRIMITIVE_FLOAT_ARRAY_TYPE = TypeToken.of(float[].class);
  public static final TypeToken<?> PRIMITIVE_DOUBLE_ARRAY_TYPE = TypeToken.of(double[].class);

  public static final TypeToken<?> CLASS_TYPE = TypeToken.of(Class.class);

  static {
    try {
      ITERATOR_RETURN_TYPE = Iterable.class.getMethod("iterator").getGenericReturnType();
      NEXT_RETURN_TYPE = Iterator.class.getMethod("next").getGenericReturnType();
      KEY_SET_RETURN_TYPE = Map.class.getMethod("keySet").getGenericReturnType();
      VALUES_RETURN_TYPE = Map.class.getMethod("values").getGenericReturnType();
    } catch (NoSuchMethodException e) {
      throw new Error(e); // should be impossible
    }
  }

  public static boolean isNullable(Class<?> clz) {
    return !isPrimitive(clz);
  }

  // sorted by size
  private static List<Class<?>> sortedPrimitiveClasses =
      Arrays.asList(
          byte.class,
          boolean.class,
          char.class,
          short.class,
          int.class,
          long.class,
          float.class,
          double.class);
  private static List<Class<?>> sortedBoxedClasses =
      Arrays.asList(
          Byte.class,
          Boolean.class,
          Character.class,
          Short.class,
          Integer.class,
          Long.class,
          Float.class,
          Double.class);
  private static int[] sortedSizes = new int[] {1, 1, 2, 2, 4, 8, 4, 8};

  public static boolean isPrimitive(Class<?> clz) {
    return sortedPrimitiveClasses.contains(clz);
  }

  public static boolean isBoxed(Class<?> clz) {
    return sortedBoxedClasses.contains(clz);
  }

  public static Class<?> boxedType(Class<?> clz) {
    Preconditions.checkArgument(clz.isPrimitive());
    int index = sortedPrimitiveClasses.indexOf(clz);
    return sortedBoxedClasses.get(index);
  }

  /** @return a primitive type class that has has max size between numericTypes */
  public static Class<?> maxType(Class<?>... numericTypes) {
    Preconditions.checkArgument(numericTypes.length >= 2);
    int maxIndex = 0;
    for (Class<?> numericType : numericTypes) {
      int index;
      if (isPrimitive(numericType)) {
        index = sortedPrimitiveClasses.indexOf(numericType);
      } else {
        index = sortedBoxedClasses.indexOf(numericType);
      }
      Preconditions.checkArgument(index != -1);
      maxIndex = Math.max(maxIndex, index);
    }
    return sortedPrimitiveClasses.get(maxIndex);
  }

  /** @return size of primitive type */
  public static int getSizeOfPrimitiveType(Class<?> numericType) {
    if (isPrimitive(numericType)) {
      int index = sortedPrimitiveClasses.indexOf(numericType);
      return sortedSizes[index];
    } else {
      String msg = String.format("Class %s must be primitive", numericType);
      throw new IllegalArgumentException(msg);
    }
  }

  /** @return default value of class */
  public static String defaultValue(Class<?> type) {
    return defaultValue(type.getSimpleName(), false);
  }

  /** @return default value of class */
  public static String defaultValue(String type) {
    return defaultValue(type, false);
  }

  /**
   * Returns the representation of default value for a given Java Type.
   *
   * @param type the string name of the Java type
   * @param typedNull if true, for null literals, return a typed (with a cast) version
   */
  public static String defaultValue(String type, boolean typedNull) {
    switch (type) {
      case JAVA_BOOLEAN:
        return "false";
      case JAVA_BYTE:
        return "(byte)0";
      case JAVA_SHORT:
        return "(short)0";
      case JAVA_INT:
        return "0";
      case JAVA_LONG:
        return "0L";
      case JAVA_FLOAT:
        return "0.0f";
      case JAVA_DOUBLE:
        return "0.0";
      default:
        if (typedNull) {
          return String.format("((%s)null)", type);
        } else {
          return "null";
        }
    }
  }

  /** @return dimensions of multi-dimension array */
  public static int getArrayDimensions(TypeToken<?> type) {
    return getArrayDimensions(type.getRawType());
  }

  /** @return dimensions of multi-dimension array */
  public static int getArrayDimensions(Class<?> type) {
    Preconditions.checkArgument(type.isArray());
    Class<?> t = type;
    int dimension = 0;
    while (t != null && t.isArray()) {
      dimension++;
      t = t.getComponentType();
    }
    return dimension;
  }

  /** @return s string that represents array type declaration of type */
  public static String getArrayType(TypeToken<?> type) {
    return getArrayType(type.getRawType());
  }

  /** @return s string that represents array type declaration of type */
  public static String getArrayType(Class<?> type) {
    Tuple2<Class<?>, Integer> arrayComponentInfo = getArrayComponentInfo(type);
    StringBuilder typeBuilder = new StringBuilder(arrayComponentInfo.f0.getCanonicalName());
    for (int i = 0; i < arrayComponentInfo.f1; i++) {
      typeBuilder.append("[]");
    }
    return typeBuilder.toString();
  }

  public static Tuple2<Class<?>, Integer> getArrayComponentInfo(Class<?> type) {
    Preconditions.checkArgument(type.isArray());
    Class<?> t = type;
    Class<?> innerType = type;
    int dimension = 0;
    while (t != null && t.isArray()) {
      dimension++;
      t = t.getComponentType();
      if (t != null) {
        innerType = t;
      }
    }
    return Tuple2.of(innerType, dimension);
  }

  /** Create an array type declaration from elemType and dimensions */
  public static String getArrayType(Class<?> elemType, int[] dimensions) {
    StringBuilder typeBuilder = new StringBuilder(elemType.getCanonicalName());
    for (int i = 0; i < dimensions.length; i++) {
      typeBuilder.append('[').append(dimensions[i]).append(']');
    }
    return typeBuilder.toString();
  }
}
