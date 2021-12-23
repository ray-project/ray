package io.ray.serialization.types;

import static io.ray.serialization.codegen.TypeUtils.BIG_DECIMAL_TYPE;
import static io.ray.serialization.codegen.TypeUtils.BOOLEAN_TYPE;
import static io.ray.serialization.codegen.TypeUtils.BYTE_TYPE;
import static io.ray.serialization.codegen.TypeUtils.CHAR_TYPE;
import static io.ray.serialization.codegen.TypeUtils.DATE_TYPE;
import static io.ray.serialization.codegen.TypeUtils.DOUBLE_TYPE;
import static io.ray.serialization.codegen.TypeUtils.FLOAT_TYPE;
import static io.ray.serialization.codegen.TypeUtils.INSTANT_TYPE;
import static io.ray.serialization.codegen.TypeUtils.INT_TYPE;
import static io.ray.serialization.codegen.TypeUtils.ITERATOR_RETURN_TYPE;
import static io.ray.serialization.codegen.TypeUtils.KEY_SET_RETURN_TYPE;
import static io.ray.serialization.codegen.TypeUtils.LOCAL_DATE_TYPE;
import static io.ray.serialization.codegen.TypeUtils.LONG_TYPE;
import static io.ray.serialization.codegen.TypeUtils.NEXT_RETURN_TYPE;
import static io.ray.serialization.codegen.TypeUtils.PRIMITIVE_BOOLEAN_TYPE;
import static io.ray.serialization.codegen.TypeUtils.PRIMITIVE_BYTE_TYPE;
import static io.ray.serialization.codegen.TypeUtils.PRIMITIVE_CHAR_TYPE;
import static io.ray.serialization.codegen.TypeUtils.PRIMITIVE_DOUBLE_TYPE;
import static io.ray.serialization.codegen.TypeUtils.PRIMITIVE_FLOAT_TYPE;
import static io.ray.serialization.codegen.TypeUtils.PRIMITIVE_INT_TYPE;
import static io.ray.serialization.codegen.TypeUtils.PRIMITIVE_LONG_TYPE;
import static io.ray.serialization.codegen.TypeUtils.PRIMITIVE_SHORT_TYPE;
import static io.ray.serialization.codegen.TypeUtils.SHORT_TYPE;
import static io.ray.serialization.codegen.TypeUtils.STRING_TYPE;
import static io.ray.serialization.codegen.TypeUtils.TIMESTAMP_TYPE;
import static io.ray.serialization.codegen.TypeUtils.VALUES_RETURN_TYPE;

import com.google.common.reflect.TypeToken;
import io.ray.serialization.util.Tuple2;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Type inference for row-format */
@SuppressWarnings("UnstableApiUsage")
public class TypeInference {
  /**
   * bean fields should all be in SUPPORTED_TYPES, enum, array/ITERABLE_TYPE/MAP_TYPE type, bean
   * type.
   *
   * <p>If bean fields is ITERABLE_TYPE/MAP_TYPE, the type should be super class(inclusive) of
   * List/Set/Map, or else should be a no arg constructor.
   */
  public static Set<TypeToken<?>> SUPPORTED_TYPES = new HashSet<>();

  static {
    SUPPORTED_TYPES.add(PRIMITIVE_BYTE_TYPE);
    SUPPORTED_TYPES.add(PRIMITIVE_BOOLEAN_TYPE);
    SUPPORTED_TYPES.add(PRIMITIVE_CHAR_TYPE);
    SUPPORTED_TYPES.add(PRIMITIVE_SHORT_TYPE);
    SUPPORTED_TYPES.add(PRIMITIVE_INT_TYPE);
    SUPPORTED_TYPES.add(PRIMITIVE_LONG_TYPE);
    SUPPORTED_TYPES.add(PRIMITIVE_FLOAT_TYPE);
    SUPPORTED_TYPES.add(PRIMITIVE_DOUBLE_TYPE);

    SUPPORTED_TYPES.add(BYTE_TYPE);
    SUPPORTED_TYPES.add(BOOLEAN_TYPE);
    SUPPORTED_TYPES.add(CHAR_TYPE);
    SUPPORTED_TYPES.add(SHORT_TYPE);
    SUPPORTED_TYPES.add(INT_TYPE);
    SUPPORTED_TYPES.add(LONG_TYPE);
    SUPPORTED_TYPES.add(FLOAT_TYPE);
    SUPPORTED_TYPES.add(DOUBLE_TYPE);

    SUPPORTED_TYPES.add(STRING_TYPE);
    SUPPORTED_TYPES.add(BIG_DECIMAL_TYPE);
    // SUPPORTED_TYPES.add(BIG_INTEGER_TYPE);
    SUPPORTED_TYPES.add(DATE_TYPE);
    SUPPORTED_TYPES.add(LOCAL_DATE_TYPE);
    SUPPORTED_TYPES.add(TIMESTAMP_TYPE);
    SUPPORTED_TYPES.add(INSTANT_TYPE);
  }

  /** @return element type of a iterable */
  public static TypeToken<?> getElementType(TypeToken<?> typeToken) {
    @SuppressWarnings("unchecked")
    TypeToken<?> supertype =
        ((TypeToken<? extends Iterable<?>>) typeToken).getSupertype(Iterable.class);
    return supertype.resolveType(ITERATOR_RETURN_TYPE).resolveType(NEXT_RETURN_TYPE);
  }

  public static TypeToken<?> getCollectionType(TypeToken<?> typeToken) {
    @SuppressWarnings("unchecked")
    TypeToken<?> supertype =
        ((TypeToken<? extends Iterable<?>>) typeToken).getSupertype(Iterable.class);
    return supertype.getSubtype(Collection.class);
  }

  /** @return key/value type of a map */
  public static Tuple2<TypeToken<?>, TypeToken<?>> getMapKeyValueType(TypeToken<?> typeToken) {
    @SuppressWarnings("unchecked")
    TypeToken<?> supertype = ((TypeToken<? extends Map<?, ?>>) typeToken).getSupertype(Map.class);
    TypeToken<?> keyType = getElementType(supertype.resolveType(KEY_SET_RETURN_TYPE));
    TypeToken<?> valueType = getElementType(supertype.resolveType(VALUES_RETURN_TYPE));
    return Tuple2.of(keyType, valueType);
  }
}
