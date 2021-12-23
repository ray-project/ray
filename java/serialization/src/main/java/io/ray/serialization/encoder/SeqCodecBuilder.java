package io.ray.serialization.encoder;

import static io.ray.serialization.codegen.Expression.Arithmetic;
import static io.ray.serialization.codegen.Expression.Cast;
import static io.ray.serialization.codegen.Expression.ForEach;
import static io.ray.serialization.codegen.Expression.ForLoop;
import static io.ray.serialization.codegen.Expression.If;
import static io.ray.serialization.codegen.Expression.Invoke;
import static io.ray.serialization.codegen.Expression.ListExpression;
import static io.ray.serialization.codegen.Expression.Literal;
import static io.ray.serialization.codegen.Expression.Reference;
import static io.ray.serialization.codegen.Expression.Return;
import static io.ray.serialization.codegen.Expression.StaticInvoke;
import static io.ray.serialization.codegen.ExpressionUtils.add;
import static io.ray.serialization.codegen.ExpressionUtils.eq;
import static io.ray.serialization.codegen.ExpressionUtils.eqNull;
import static io.ray.serialization.codegen.ExpressionUtils.not;
import static io.ray.serialization.codegen.ExpressionUtils.nullValue;
import static io.ray.serialization.codegen.TypeUtils.CLASS_TYPE;
import static io.ray.serialization.codegen.TypeUtils.COLLECTION_TYPE;
import static io.ray.serialization.codegen.TypeUtils.MAP_TYPE;
import static io.ray.serialization.codegen.TypeUtils.OBJECT_TYPE;
import static io.ray.serialization.codegen.TypeUtils.PRIMITIVE_BOOLEAN_TYPE;
import static io.ray.serialization.codegen.TypeUtils.PRIMITIVE_BYTE_TYPE;
import static io.ray.serialization.codegen.TypeUtils.PRIMITIVE_CHAR_TYPE;
import static io.ray.serialization.codegen.TypeUtils.PRIMITIVE_DOUBLE_TYPE;
import static io.ray.serialization.codegen.TypeUtils.PRIMITIVE_FLOAT_TYPE;
import static io.ray.serialization.codegen.TypeUtils.PRIMITIVE_INT_TYPE;
import static io.ray.serialization.codegen.TypeUtils.PRIMITIVE_LONG_TYPE;
import static io.ray.serialization.codegen.TypeUtils.PRIMITIVE_SHORT_TYPE;
import static io.ray.serialization.codegen.TypeUtils.PRIMITIVE_VOID_TYPE;
import static io.ray.serialization.codegen.TypeUtils.SET_TYPE;
import static io.ray.serialization.codegen.TypeUtils.getSizeOfPrimitiveType;
import static io.ray.serialization.codegen.TypeUtils.isBoxed;
import static io.ray.serialization.codegen.TypeUtils.isPrimitive;
import static io.ray.serialization.encoder.CodecUtils.getPackage;
import static io.ray.serialization.types.TypeInference.getElementType;
import static io.ray.serialization.util.StringUtils.format;
import static io.ray.serialization.util.StringUtils.uncapitalize;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import io.ray.serialization.Fury;
import io.ray.serialization.codegen.CodegenContext;
import io.ray.serialization.codegen.Expression;
import io.ray.serialization.resolver.ClassResolver;
import io.ray.serialization.resolver.ReferenceResolver;
import io.ray.serialization.serializers.CodegenSerializer;
import io.ray.serialization.serializers.CollectionSerializers.CollectionSerializer;
import io.ray.serialization.serializers.MapSerializers.MapSerializer;
import io.ray.serialization.serializers.Serializer;
import io.ray.serialization.types.TypeInference;
import io.ray.serialization.util.Descriptor;
import io.ray.serialization.util.LoggerFactory;
import io.ray.serialization.util.MemoryBuffer;
import io.ray.serialization.util.ReflectionUtils;
import io.ray.serialization.util.Tuple2;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.slf4j.Logger;

/**
 * Generate sequential read/write code for java serialization to speed up performance. It also
 * reduce space overhead introduced by aligning. Codegen only for time-consuming field, others
 * delegate to fury.
 */
@SuppressWarnings("UnstableApiUsage")
public class SeqCodecBuilder extends CodecBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(SeqCodecBuilder.class);
  public static final String BUFFER_NAME = "buffer";
  public static final String REF_RESOLVER_NAME = "refResolver";
  public static final String CLASS_RESOLVER_NAME = "classResolver";
  public static final String POJO_CLASS_TYPE_NAME = "classType";
  private static final TypeToken<?> CLASS_RESOLVER_TYPE_TOKEN = TypeToken.of(ClassResolver.class);

  private final Reference refResolverRef;
  private final Reference classResolverRef =
      new Reference(CLASS_RESOLVER_NAME, CLASS_RESOLVER_TYPE_TOKEN);
  private final Map<Class<?>, Reference> serializerMap = new HashMap<>();
  private final Fury fury;
  private Literal classVersionHash;

  // sorted primitive descriptors from largest to smallest, if size is the same, sort by field name
  // to fix order
  protected final List<Descriptor> sortedPrimitiveDescriptors;
  protected final List<Descriptor> nonPrimitiveDescriptors;

  public SeqCodecBuilder(Class<?> beanClass, Fury fury) {
    this(TypeToken.of(beanClass), fury);
  }

  public SeqCodecBuilder(TypeToken<?> beanType, Fury fury) {
    super(new CodegenContext(), beanType);
    this.fury = fury;
    classVersionHash =
        new Literal(CodegenSerializer.computeVersionHash(beanClass), PRIMITIVE_INT_TYPE);
    Tuple2<List<Descriptor>, List<Descriptor>> sortedDescriptors =
        buildDescriptors(beanType.getRawType());
    sortedPrimitiveDescriptors = sortedDescriptors.f0;
    nonPrimitiveDescriptors = sortedDescriptors.f1;

    TypeToken<?> refResolverTypeToken = TypeToken.of(fury.getReferenceResolver().getClass());
    refResolverRef = new Reference(REF_RESOLVER_NAME, refResolverTypeToken, false);
    Expression refResolverExpr =
        new Invoke(furyRef, "getReferenceResolver", TypeToken.of(ReferenceResolver.class));
    ctx.addField(
        ctx.type(refResolverTypeToken),
        REF_RESOLVER_NAME,
        new Cast(refResolverExpr, refResolverTypeToken));

    Expression classResolverExpr =
        new Invoke(furyRef, "getClassResolver", CLASS_RESOLVER_TYPE_TOKEN);
    ctx.addField(ctx.type(CLASS_RESOLVER_TYPE_TOKEN), CLASS_RESOLVER_NAME, classResolverExpr);
  }

  static Tuple2<List<Descriptor>, List<Descriptor>> buildDescriptors(Class<?> cls) {
    List<Descriptor> descriptors = Descriptor.getDescriptors(cls);
    // sort primitive descriptors from largest to smallest, if size is the same,
    // sort by field name to fix order.
    TreeSet<Descriptor> primitiveDescriptors =
        new TreeSet<>(
            (d1, d2) -> {
              int c =
                  getSizeOfPrimitiveType(d2.getTypeToken().getRawType())
                      - getSizeOfPrimitiveType(d1.getTypeToken().getRawType());
              if (c == 0) {
                return d2.getName().compareTo(d1.getName());
              } else {
                return c;
              }
            });
    TreeSet<Descriptor> otherDescriptors =
        new TreeSet<>(
            (d1, d2) -> {
              // sort by type so that we can hit class info cache more possibly.
              // sort by field name to fix order if type is same.
              int c =
                  d2.getTypeToken()
                      .getType()
                      .getTypeName()
                      .compareTo(d2.getTypeToken().getType().getTypeName());
              if (c == 0) {
                return d2.getName().compareTo(d1.getName());
              } else {
                return c;
              }
            });
    for (Descriptor descriptor : descriptors) {
      if (descriptor.getTypeToken().isPrimitive()) {
        primitiveDescriptors.add(descriptor);
      } else {
        otherDescriptors.add(descriptor);
      }
    }

    // getter/setter may lose some inner state of an object, so we set them to null.
    List<Descriptor> primitiveDescriptorsList =
        primitiveDescriptors.stream()
            .map(d -> new Descriptor(d.getField(), d.getTypeToken(), null, null))
            .collect(Collectors.toList());
    List<Descriptor> otherDescriptorsList =
        otherDescriptors.stream()
            .map(
                d -> {
                  if (!Modifier.isPublic(d.getTypeToken().getRawType().getModifiers())) {
                    // Non-public class can't accessed from generated code.
                    // (Ignore protected/package level access for simplicity.
                    // Since class members whose type are non-public class are rare,
                    // it doesn't have much impact on performance.)
                    return new Descriptor(d.getField(), OBJECT_TYPE, null, null);
                  } else {
                    return new Descriptor(d.getField(), d.getTypeToken(), null, null);
                  }
                })
            .collect(Collectors.toList());
    return Tuple2.of(primitiveDescriptorsList, otherDescriptorsList);
  }

  public String codecClassName(Class<?> beanClass) {
    String name = ReflectionUtils.getClassNameWithoutPackage(beanClass);
    if (fury.isReferenceTracking()) {
      // Generated classes are different when referenceTracking is switched.
      // So we need to use a different name.
      name += "FurySeqRefCodec";
    } else {
      name += "FurySeqCodec";
    }
    return name.replace("$", "_");
  }

  public String codecQualifiedClassName(Class<?> beanClass) {
    return getPackage(beanClass) + "." + codecClassName(beanClass);
  }

  @Override
  public String genCode() {
    ctx.setPackage(getPackage(beanClass));
    ctx.addImport(Serializer.class);
    ctx.addImport(CollectionSerializer.class);
    ctx.addImport(MapSerializer.class);
    ctx.addImport(ReferenceResolver.class);
    ctx.addImport(ClassResolver.class);
    ctx.addImport(CodegenSerializer.class);
    String className = codecClassName(beanClass);
    ctx.setClassName(className);
    // don't addImport(beanClass), because user class may name collide.
    ctx.extendsClasses(ctx.type(Generated.GeneratedSerializer.class));
    ctx.reserveName(POJO_CLASS_TYPE_NAME);
    ctx.addField(ctx.type(Fury.class), FURY_NAME);
    String constructorCode =
        format(
            "super(${fury}, ${cls});\n" + "this.${fury} = ${fury};\n",
            "fury",
            FURY_NAME,
            "cls",
            POJO_CLASS_TYPE_NAME);
    ctx.addConstructor(constructorCode, Fury.class, "fury", Class.class, POJO_CLASS_TYPE_NAME);
    Expression encodeExpr = buildEncodeExpression();
    Expression decodeExpr = buildDecodeExpression();

    String encodeCode = encodeExpr.genCode(ctx).code();
    String decodeCode = decodeExpr.genCode(ctx).code();
    ctx.overrideMethod(
        "write",
        encodeCode,
        void.class,
        Fury.class,
        FURY_NAME,
        MemoryBuffer.class,
        BUFFER_NAME,
        Object.class,
        ROOT_OBJECT_NAME);
    ctx.overrideMethod(
        "read",
        decodeCode,
        Object.class,
        Fury.class,
        FURY_NAME,
        MemoryBuffer.class,
        BUFFER_NAME,
        Class.class,
        POJO_CLASS_TYPE_NAME);

    long startTime = System.nanoTime();
    String code = ctx.genCode();
    long durationMs = (System.nanoTime() - startTime) / 1000;
    LOG.debug(
        "Generate {} for class {} take {} us",
        Serializer.class.getSimpleName(),
        beanClass,
        durationMs);
    return code;
  }

  /**
   * Return an expression that serialize java bean of type {@link CodecBuilder#beanClass} to buffer
   */
  @Override
  public Expression buildEncodeExpression() {
    Reference inputObject = new Reference(ROOT_OBJECT_NAME, OBJECT_TYPE, false);
    Reference buffer = new Reference(BUFFER_NAME, bufferTypeToken, false);

    ListExpression expressions = new ListExpression();
    Expression bean = new Cast(inputObject, beanType, ctx.freshName(beanClass));
    expressions.add(bean);
    if (fury.checkClassVersion()) {
      expressions.add(new Invoke(buffer, "writeInt", classVersionHash));
    }
    expressions.addAll(serializePrimitives(bean, buffer));
    for (Descriptor d : nonPrimitiveDescriptors) {
      TypeToken<?> fieldType = d.getTypeToken();
      Expression fieldValue = getFieldValue(bean, d);
      Expression fieldExpr = serializeFor(fieldValue, buffer, fieldType);
      expressions.add(fieldExpr);
    }
    return expressions;
  }

  /**
   * Return a list of expressions that serialize all primitive fields. This can reduce unnecessary
   * grow call and increment writerIndex in writeXXX.
   */
  private List<Expression> serializePrimitives(Expression bean, Expression buffer) {
    List<Expression> expressions = new ArrayList<>();
    int totalSize =
        sortedPrimitiveDescriptors.stream()
            .mapToInt(d -> getSizeOfPrimitiveType(d.getTypeToken().getRawType()))
            .sum();
    Literal totalSizeLiteral = new Literal(totalSize, PRIMITIVE_INT_TYPE);
    Expression writerIndex = new Invoke(buffer, "writerIndex", "writerIndex", PRIMITIVE_INT_TYPE);
    expressions.add(writerIndex);
    expressions.add(new Invoke(buffer, "grow", totalSizeLiteral));
    int acc = 0;
    for (Descriptor descriptor : sortedPrimitiveDescriptors) {
      Class<?> clz = descriptor.getTypeToken().getRawType();
      Preconditions.checkArgument(isPrimitive(clz));
      Expression fieldValue = getFieldValue(bean, descriptor);
      if (clz == byte.class) {
        Arithmetic index = add(writerIndex, new Literal(acc, PRIMITIVE_INT_TYPE));
        expressions.add(new Invoke(buffer, "put", index, fieldValue));
        acc += 1;
      } else if (clz == boolean.class) {
        Arithmetic index = add(writerIndex, new Literal(acc, PRIMITIVE_INT_TYPE));
        expressions.add(new Invoke(buffer, "putBoolean", index, fieldValue));
        acc += 1;
      } else if (clz == char.class) {
        Arithmetic index = add(writerIndex, new Literal(acc, PRIMITIVE_INT_TYPE));
        expressions.add(new Invoke(buffer, "putChar", index, fieldValue));
        acc += 2;
      } else if (clz == short.class) {
        Arithmetic index = add(writerIndex, new Literal(acc, PRIMITIVE_INT_TYPE));
        expressions.add(new Invoke(buffer, "putShort", index, fieldValue));
        acc += 2;
      } else if (clz == int.class) {
        Arithmetic index = add(writerIndex, new Literal(acc, PRIMITIVE_INT_TYPE));
        expressions.add(new Invoke(buffer, "putInt", index, fieldValue));
        acc += 4;
      } else if (clz == long.class) {
        Arithmetic index = add(writerIndex, new Literal(acc, PRIMITIVE_INT_TYPE));
        expressions.add(new Invoke(buffer, "putLong", index, fieldValue));
        acc += 8;
      } else if (clz == float.class) {
        Arithmetic index = add(writerIndex, new Literal(acc, PRIMITIVE_INT_TYPE));
        expressions.add(new Invoke(buffer, "putFloat", index, fieldValue));
        acc += 4;
      } else if (clz == double.class) {
        Arithmetic index = add(writerIndex, new Literal(acc, PRIMITIVE_INT_TYPE));
        expressions.add(new Invoke(buffer, "putDouble", index, fieldValue));
        acc += 8;
      } else {
        throw new IllegalStateException("impossible");
      }
    }

    Expression newWriterIndex = add(writerIndex, new Literal(totalSizeLiteral, PRIMITIVE_INT_TYPE));
    Expression setWriterIndex = new Invoke(buffer, "writerIndex", newWriterIndex);
    expressions.add(setWriterIndex);
    return expressions;
  }

  /**
   * Return an expression that serialize an nullable <code>inputObject</code> to <code>buffer</code>
   */
  private Expression serializeFor(
      Expression inputObject, Expression buffer, TypeToken<?> typeToken) {
    if (isPrimitive(typeToken.getRawType())) {
      return serializeForNotNull(inputObject, buffer, typeToken);
    } else {
      if (fury.isReferenceTracking()) {
        return new If(
            not(writeReferenceOrNull(buffer, inputObject)),
            serializeForNotNull(inputObject, buffer, typeToken));
      } else {
        Expression action =
            new ListExpression(
                new Invoke(buffer, "writeByte", new Literal(Fury.NOT_NULL, PRIMITIVE_BYTE_TYPE)),
                serializeForNotNull(inputObject, buffer, typeToken));
        return new If(
            eqNull(inputObject),
            new Invoke(buffer, "writeByte", new Literal(Fury.NULL, PRIMITIVE_BYTE_TYPE)),
            action);
      }
    }
  }

  private Expression writeReferenceOrNull(Expression buffer, Expression object) {
    return new Invoke(
        refResolverRef,
        "writeReferenceOrNull",
        "notWrite",
        PRIMITIVE_BOOLEAN_TYPE,
        false,
        buffer,
        object);
  }

  /**
   * Return an expression that serialize an not null <code>inputObject</code> to <code>buffer</code>
   */
  private Expression serializeForNotNull(
      Expression inputObject, Expression buffer, TypeToken<?> typeToken) {
    Class<?> clz = typeToken.getRawType();
    if (isPrimitive(clz) || isBoxed(clz)) {
      // for primitive, inline call here to avoid java boxing, rather call corresponding serializer.
      if (clz == byte.class || clz == Byte.class) {
        return new Invoke(buffer, "writeByte", inputObject);
      } else if (clz == boolean.class || clz == Boolean.class) {
        return new Invoke(buffer, "writeBoolean", inputObject);
      } else if (clz == char.class || clz == Character.class) {
        return new Invoke(buffer, "writeChar", inputObject);
      } else if (clz == short.class || clz == Short.class) {
        return new Invoke(buffer, "writeShort", inputObject);
      } else if (clz == int.class || clz == Integer.class) {
        return new Invoke(buffer, "writeInt", inputObject);
      } else if (clz == long.class || clz == Long.class) {
        return new Invoke(buffer, "writeLong", inputObject);
      } else if (clz == float.class || clz == Float.class) {
        return new Invoke(buffer, "writeFloat", inputObject);
      } else if (clz == double.class || clz == Double.class) {
        return new Invoke(buffer, "writeDouble", inputObject);
      } else {
        throw new IllegalStateException("impossible");
      }
    } else {
      Expression action;
      // this is different from ITERABLE_TYPE in RowCodecBuilder. In row-format we don't need to
      // ensure
      // class consistence, we only need to ensure interface consistence. But in java serialization,
      // we need to ensure class consistence.
      if (COLLECTION_TYPE.isSupertypeOf(typeToken)) {
        action = serializeForCollection(buffer, inputObject, typeToken);
      } else if (MAP_TYPE.isSupertypeOf(typeToken)) {
        action = serializeForMap(buffer, inputObject, typeToken);
      } else {
        if (Modifier.isFinal(clz.getModifiers())) {
          Expression serializer = getOrCreateSerializer(clz);
          action = new Invoke(serializer, "write", furyRef, buffer, inputObject);
        } else {
          action = new Invoke(furyRef, "serializeNonReferenceToJava", buffer, inputObject);
        }
      }
      return action;
    }
  }

  /**
   * Return an serializer expression which will be used to call write/read method to avoid virtual
   * methods calls in most situations
   */
  private Expression getOrCreateSerializer(Class<?> cls) {
    Reference serializerRef = serializerMap.get(cls);
    if (serializerRef == null) {
      // potential recursive call for seq codec generation is handled in `getSerializerClass`.
      Class<? extends Serializer> serializerClass = fury.getClassResolver().getSerializerClass(cls);
      Preconditions.checkNotNull(serializerClass, "Unsupported for class " + cls);
      TypeToken<? extends Serializer> serializerTypeToken = TypeToken.of(serializerClass);
      Literal clzLiteral = new Literal(ctx.type(cls) + ".class");
      Literal serializerClassLiteral = new Literal(ctx.type(serializerClass) + ".class");
      Expression newSerializerExpr =
          new StaticInvoke(
              Serializer.class,
              "newSerializer",
              "serializer",
              TypeToken.of(Serializer.class),
              false,
              furyRef,
              clzLiteral,
              serializerClassLiteral);
      String name = ctx.freshName(uncapitalize(serializerClass.getSimpleName()));
      ctx.addField(
          ctx.type(serializerClass), name, new Cast(newSerializerExpr, serializerTypeToken));
      serializerRef = new Reference(name, serializerTypeToken, false);
      serializerMap.put(cls, serializerRef);
    }
    return serializerRef;
  }

  /**
   * Return an expression to write a collection to <code>buffer</code>. This expression can have
   * better efficiency for final element type. For final element type, it doesn't have to write
   * class info, no need to forward to <code>fury</code>.
   */
  private Expression serializeForCollection(
      Expression buffer, Expression collection, TypeToken<?> typeToken) {
    TypeToken<?> elementType = getElementType(typeToken);
    if (!Modifier.isFinal(elementType.getRawType().getModifiers())) {
      return new Invoke(furyRef, "serializeNonReferenceToJava", buffer, collection);
    } else {
      ListExpression actions = new ListExpression();
      Expression serializer;
      Class<?> clz = typeToken.getRawType();
      if (Modifier.isFinal(clz.getModifiers())) {
        serializer = getOrCreateSerializer(clz);
      } else {
        Expression clsExpr = new Invoke(collection, "getClass", "cls", CLASS_TYPE);
        // write collection class info to buffer when collection class is not final.
        actions.add(new Invoke(classResolverRef, "writeClass", buffer, clsExpr));
        serializer =
            new Invoke(
                classResolverRef,
                "getSerializer",
                "serializer",
                TypeToken.of(Serializer.class),
                false,
                clsExpr);
        serializer =
            new Cast(serializer, TypeToken.of(CollectionSerializer.class), "collectionSerializer");
      }
      Invoke supportHook =
          new Invoke(serializer, "supportCodegenHook", "supportHook", PRIMITIVE_BOOLEAN_TYPE);
      Invoke size = new Invoke(collection, "size", PRIMITIVE_INT_TYPE);
      Invoke writeSize = new Invoke(buffer, "writeInt", size);
      Invoke writeHeader = new Invoke(serializer, "writeHeader", furyRef, buffer, collection);
      ForEach writeElements =
          new ForEach(collection, (i, value) -> serializeFor(value, buffer, elementType));
      Expression hookWrite = new ListExpression(writeSize, writeHeader, writeElements);
      Expression write =
          new If(
              supportHook, hookWrite, new Invoke(serializer, "write", furyRef, buffer, collection));
      return actions.add(write);
    }
  }

  /**
   * Return an expression to write a map to <code>buffer</code>. This expression can have better
   * efficiency for final key/value type. For final key/value type, it doesn't have to write class
   * info, no need to forward to <code>fury</code>.
   */
  private Expression serializeForMap(Expression buffer, Expression map, TypeToken<?> typeToken) {
    Tuple2<TypeToken<?>, TypeToken<?>> keyValueType = TypeInference.getMapKeyValueType(typeToken);
    TypeToken<?> keyType = keyValueType.f0;
    TypeToken<?> valueType = keyValueType.f1;
    if (!Modifier.isFinal(keyType.getRawType().getModifiers())
        && !Modifier.isFinal(valueType.getRawType().getModifiers())) {
      return new Invoke(furyRef, "serializeNonReferenceToJava", buffer, map);
    } else {
      ListExpression actions = new ListExpression();
      Expression serializer;
      Class<?> clz = typeToken.getRawType();
      if (Modifier.isFinal(clz.getModifiers())) {
        serializer = getOrCreateSerializer(clz);
      } else {
        Expression clsExpr = new Invoke(map, "getClass", CLASS_TYPE);
        // write map class info to buffer when map class is not final.
        actions.add(new Invoke(classResolverRef, "writeClass", buffer, clsExpr));
        serializer =
            new Invoke(classResolverRef, "getSerializer", TypeToken.of(Serializer.class), clsExpr);
        serializer = new Cast(serializer, TypeToken.of(MapSerializer.class), "mapSerializer");
      }
      Invoke supportCodegenHook =
          new Invoke(serializer, "supportCodegenHook", PRIMITIVE_BOOLEAN_TYPE);
      Invoke size = new Invoke(map, "size", PRIMITIVE_INT_TYPE);
      Invoke writeSize = new Invoke(buffer, "writeInt", size);
      Invoke writeHeader = new Invoke(serializer, "writeHeader", furyRef, buffer, map);
      Invoke entrySet = new Invoke(map, "entrySet", "entrySet", SET_TYPE);
      ForEach writeKeyValues =
          new ForEach(
              entrySet,
              (i, entryObj) -> {
                Expression entry = new Cast(entryObj, TypeToken.of(Map.Entry.class), "entry");
                Expression key = new Invoke(entry, "getKey", "keyObj", OBJECT_TYPE);
                key = new Cast(key, keyType, "key");
                Expression value = new Invoke(entry, "getValue", "valueObj", OBJECT_TYPE);
                value = new Cast(value, valueType, "value");
                return new ListExpression(
                    serializeFor(key, buffer, keyType), serializeFor(value, buffer, valueType));
              });
      Expression hookWrite = new ListExpression(writeSize, writeHeader, writeKeyValues);
      Expression write =
          new If(
              supportCodegenHook, hookWrite, new Invoke(serializer, "write", furyRef, buffer, map));
      return actions.add(write);
    }
  }

  /**
   * Return an expression that deserialize data as a java bean of type {@link
   * CodecBuilder#beanClass} from <code>buffer</code>
   */
  public Expression buildDecodeExpression() {
    Reference buffer = new Reference(BUFFER_NAME, bufferTypeToken, false);
    ListExpression expressions = new ListExpression();
    if (fury.checkClassVersion()) {
      expressions.add(checkClassVersion(buffer));
    }
    Expression bean = newBean();
    Expression referenceObject = new Invoke(refResolverRef, "reference", PRIMITIVE_VOID_TYPE, bean);
    expressions.add(bean);
    expressions.add(referenceObject);
    expressions.addAll(deserializePrimitives(bean, buffer));
    for (Descriptor d : nonPrimitiveDescriptors) {
      Expression action;
      if (d.getTypeToken().isPrimitive()) {
        action = setFieldValue(bean, d, deserializeForNotNull(buffer, d.getTypeToken()));
      } else {
        if (fury.isReferenceTracking()) {
          Expression tag = readReferenceOrNull(buffer);
          // indicates that the object is first read.
          Expression needDeserialize =
              eq(tag, new Literal(Fury.NOT_NULL, PRIMITIVE_BYTE_TYPE), "needDeserialize");
          Expression refId = new Invoke(refResolverRef, "preserveReferenceId", PRIMITIVE_INT_TYPE);
          Expression deserializedValue = deserializeForNotNull(buffer, d.getTypeToken());
          Expression setReadObject =
              new Invoke(refResolverRef, "setReadObject", refId, deserializedValue);
          Expression readObject = new Invoke(refResolverRef, "getReadObject", OBJECT_TYPE);
          // use false to ignore null
          action =
              new If(
                  needDeserialize,
                  new ListExpression(
                      refId,
                      deserializedValue,
                      setReadObject,
                      setFieldValue(bean, d, deserializedValue)),
                  new If(
                      not(eqNull(readObject)),
                      setFieldValue(bean, d, new Cast(readObject, d.getTypeToken()))));
        } else {
          Invoke nullTag = new Invoke(buffer, "readByte", "nullTag", PRIMITIVE_BYTE_TYPE);
          Expression notNull =
              eq(nullTag, new Literal(Fury.NOT_NULL, PRIMITIVE_BYTE_TYPE), "notNull");
          action =
              new If(
                  notNull, setFieldValue(bean, d, deserializeForNotNull(buffer, d.getTypeToken())));
        }
      }
      expressions.add(action);
    }
    expressions.add(new Return(bean));
    return expressions;
  }

  private Expression checkClassVersion(Expression buffer) {
    Expression hash = new Invoke(buffer, "readInt", PRIMITIVE_INT_TYPE);
    return new StaticInvoke(
        CodegenSerializer.class,
        "checkClassVersion",
        PRIMITIVE_VOID_TYPE,
        false,
        furyRef,
        hash,
        classVersionHash);
  }

  private Expression readReferenceOrNull(Expression buffer) {
    return new Invoke(
        refResolverRef, "readReferenceOrNull", "tag", PRIMITIVE_BYTE_TYPE, false, buffer);
  }

  /**
   * Return a list of expressions that deserialize all primitive fields. This can reduce unnecessary
   * check call and increment readerIndex in writeXXX.
   */
  private List<Expression> deserializePrimitives(Expression bean, Expression buffer) {
    List<Expression> expressions = new ArrayList<>();
    int totalSize =
        sortedPrimitiveDescriptors.stream()
            .mapToInt(d -> getSizeOfPrimitiveType(d.getTypeToken().getRawType()))
            .sum();
    Literal totalSizeLiteral = new Literal(totalSize, PRIMITIVE_INT_TYPE);
    Expression readerIndex = new Invoke(buffer, "readerIndex", "readerIndex", PRIMITIVE_INT_TYPE);
    expressions.add(readerIndex);
    expressions.add(new Invoke(buffer, "checkReadableBytes", totalSizeLiteral));
    int acc = 0;
    for (Descriptor descriptor : sortedPrimitiveDescriptors) {
      TypeToken<?> type = descriptor.getTypeToken();
      Class<?> clz = type.getRawType();
      Preconditions.checkArgument(isPrimitive(clz));
      Expression fieldValue;
      if (clz == byte.class) {
        Arithmetic index = add(readerIndex, new Literal(acc, PRIMITIVE_INT_TYPE));
        fieldValue = new Invoke(buffer, "get", PRIMITIVE_BYTE_TYPE, index);
        acc += 1;
      } else if (clz == boolean.class) {
        Arithmetic index = add(readerIndex, new Literal(acc, PRIMITIVE_INT_TYPE));
        fieldValue = new Invoke(buffer, "getBoolean", PRIMITIVE_BOOLEAN_TYPE, index);
        acc += 1;
      } else if (clz == char.class) {
        Arithmetic index = add(readerIndex, new Literal(acc, PRIMITIVE_INT_TYPE));
        fieldValue = new Invoke(buffer, "getChar", PRIMITIVE_CHAR_TYPE, index);
        acc += 2;
      } else if (clz == short.class) {
        Arithmetic index = add(readerIndex, new Literal(acc, PRIMITIVE_INT_TYPE));
        fieldValue = new Invoke(buffer, "getShort", PRIMITIVE_SHORT_TYPE, index);
        acc += 2;
      } else if (clz == int.class) {
        Arithmetic index = add(readerIndex, new Literal(acc, PRIMITIVE_INT_TYPE));
        fieldValue = new Invoke(buffer, "getInt", PRIMITIVE_INT_TYPE, index);
        acc += 4;
      } else if (clz == long.class) {
        Arithmetic index = add(readerIndex, new Literal(acc, PRIMITIVE_INT_TYPE));
        fieldValue = new Invoke(buffer, "getLong", PRIMITIVE_LONG_TYPE, index);
        acc += 8;
      } else if (clz == float.class) {
        Arithmetic index = add(readerIndex, new Literal(acc, PRIMITIVE_INT_TYPE));
        fieldValue = new Invoke(buffer, "getFloat", PRIMITIVE_FLOAT_TYPE, index);
        acc += 4;
      } else if (clz == double.class) {
        Arithmetic index = add(readerIndex, new Literal(acc, PRIMITIVE_INT_TYPE));
        fieldValue = new Invoke(buffer, "getDouble", PRIMITIVE_DOUBLE_TYPE, index);
        acc += 8;
      } else {
        throw new IllegalStateException("impossible");
      }
      expressions.add(setFieldValue(bean, descriptor, fieldValue));
    }

    Expression newReaderIndex = add(readerIndex, new Literal(totalSizeLiteral, PRIMITIVE_INT_TYPE));
    Expression setReaderIndex = new Invoke(buffer, "readerIndex", newReaderIndex);
    expressions.add(setReaderIndex);
    return expressions;
  }

  /**
   * Return an expression that deserialize an nullable <code>inputObject</code> from <code>buffer
   * </code>
   */
  private Expression deserializeFor(Expression buffer, TypeToken<?> typeToken) {
    if (fury.isReferenceTracking()) {
      Expression tag = readReferenceOrNull(buffer);
      // indicates that the object is first read.
      Expression needDeserialize =
          eq(tag, new Literal(Fury.NOT_NULL, PRIMITIVE_BYTE_TYPE), "needDeserialize");
      Expression refId = new Invoke(refResolverRef, "preserveReferenceId", PRIMITIVE_INT_TYPE);
      Expression deserializedValue = deserializeForNotNull(buffer, typeToken);
      Expression setReadObject =
          new Invoke(refResolverRef, "setReadObject", refId, deserializedValue);
      Cast readValue =
          new Cast(new Invoke(refResolverRef, "getReadObject", OBJECT_TYPE, false), typeToken);
      // use false to ignore null
      return new If(
          needDeserialize,
          new ListExpression(refId, deserializedValue, setReadObject, deserializedValue),
          readValue,
          false);
    } else {
      Invoke nullTag = new Invoke(buffer, "readByte", "nullTag", PRIMITIVE_BYTE_TYPE);
      Expression notNull = eq(nullTag, new Literal(Fury.NOT_NULL, PRIMITIVE_BYTE_TYPE), "notNull");
      Expression value = deserializeForNotNull(buffer, typeToken);
      // use false to ignore null.
      return new If(notNull, value, nullValue(typeToken), false);
    }
  }

  /**
   * Return an expression that deserialize an not null <code>inputObject</code> from <code>buffer
   * </code>
   */
  private Expression deserializeForNotNull(Expression buffer, TypeToken<?> typeToken) {
    Class<?> clz = typeToken.getRawType();
    if (isPrimitive(clz) || isBoxed(clz)) {
      // for primitive, inline call here to avoid java boxing, rather call corresponding serializer.
      if (clz == byte.class || clz == Byte.class) {
        return new Invoke(buffer, "readByte", PRIMITIVE_BYTE_TYPE);
      } else if (clz == boolean.class || clz == Boolean.class) {
        return new Invoke(buffer, "readBoolean", PRIMITIVE_BOOLEAN_TYPE);
      } else if (clz == char.class || clz == Character.class) {
        return new Invoke(buffer, "readChar", TypeToken.of(char.class));
      } else if (clz == short.class || clz == Short.class) {
        return new Invoke(buffer, "readShort", PRIMITIVE_SHORT_TYPE);
      } else if (clz == int.class || clz == Integer.class) {
        return new Invoke(buffer, "readInt", PRIMITIVE_INT_TYPE);
      } else if (clz == long.class || clz == Long.class) {
        return new Invoke(buffer, "readLong", PRIMITIVE_LONG_TYPE);
      } else if (clz == float.class || clz == Float.class) {
        return new Invoke(buffer, "readFloat", PRIMITIVE_FLOAT_TYPE);
      } else if (clz == double.class || clz == Double.class) {
        return new Invoke(buffer, "readDouble", PRIMITIVE_DOUBLE_TYPE);
      } else {
        throw new IllegalStateException("impossible");
      }
    } else {
      Expression obj;
      if (COLLECTION_TYPE.isSupertypeOf(typeToken)) {
        obj = deserializeForCollection(buffer, typeToken);
      } else if (MAP_TYPE.isSupertypeOf(typeToken)) {
        obj = deserializeForMap(buffer, typeToken);
      } else {
        if (Modifier.isFinal(clz.getModifiers())) {
          Expression serializer = getOrCreateSerializer(clz);
          Literal clzLiteral = new Literal(ctx.type(clz) + ".class");
          Class<?> returnType =
              ReflectionUtils.getReturnType(serializer.type().getRawType(), "read");
          obj =
              new Invoke(serializer, "read", TypeToken.of(returnType), furyRef, buffer, clzLiteral);
        } else {
          obj = new Invoke(furyRef, "deserializeNonReferenceFromJava", OBJECT_TYPE, false, buffer);
        }
      }
      if (clz.isAssignableFrom(obj.type().getRawType())) {
        return obj;
      } else {
        return new Cast(obj, typeToken);
      }
    }
  }

  /**
   * Return an expression to deserialize a collection from <code>buffer</code>. Must keep consistent
   * with {@link SeqCodecBuilder#serializeForCollection}
   */
  private Expression deserializeForCollection(Expression buffer, TypeToken<?> typeToken) {
    TypeToken<?> elementType = getElementType(typeToken);
    if (!Modifier.isFinal(elementType.getRawType().getModifiers())) {
      return new Invoke(furyRef, "deserializeNonReferenceFromJava", OBJECT_TYPE, false, buffer);
    } else {
      Expression clsExpr, serializer;
      Class<?> clz = typeToken.getRawType();
      if (Modifier.isFinal(clz.getModifiers())) {
        clsExpr = new Literal(ctx.type(clz) + ".class");
        serializer = getOrCreateSerializer(clz);
      } else {
        clsExpr = new Invoke(classResolverRef, "readClass", "cls", CLASS_TYPE, false, buffer);
        serializer =
            new Invoke(
                classResolverRef,
                "getSerializer",
                "serializer",
                TypeToken.of(Serializer.class),
                false,
                clsExpr);
        serializer =
            new Cast(serializer, TypeToken.of(CollectionSerializer.class), "collectionSerializer");
      }
      Invoke supportHook =
          new Invoke(serializer, "supportCodegenHook", "supportHook", PRIMITIVE_BOOLEAN_TYPE);
      Expression size = new Invoke(buffer, "readInt", "size", PRIMITIVE_INT_TYPE);
      Expression newCollection =
          new Invoke(serializer, "newCollection", COLLECTION_TYPE, furyRef, buffer, clsExpr, size);
      Expression start = new Literal(0, PRIMITIVE_INT_TYPE);
      Expression step = new Literal(1, PRIMITIVE_INT_TYPE);
      ForLoop readElements =
          new ForLoop(
              start,
              size,
              step,
              i -> new Invoke(newCollection, "add", deserializeFor(buffer, elementType)));
      // place newCollection as last as expr value
      Expression hookRead = new ListExpression(size, newCollection, readElements, newCollection);
      return new If(
          supportHook,
          hookRead,
          new Invoke(serializer, "read", COLLECTION_TYPE, furyRef, buffer, clsExpr),
          false);
    }
  }

  /**
   * Return an expression to deserialize a map from <code>buffer</code>. Must keep consistent with
   * {@link SeqCodecBuilder#serializeForMap}
   */
  private Expression deserializeForMap(Expression buffer, TypeToken<?> typeToken) {
    Tuple2<TypeToken<?>, TypeToken<?>> keyValueType = TypeInference.getMapKeyValueType(typeToken);
    TypeToken<?> keyType = keyValueType.f0;
    TypeToken<?> valueType = keyValueType.f1;
    if (!Modifier.isFinal(keyType.getRawType().getModifiers())
        && !Modifier.isFinal(valueType.getRawType().getModifiers())) {
      return new Invoke(furyRef, "deserializeNonReferenceFromJava", OBJECT_TYPE, false, buffer);
    } else {
      Expression clsExpr, serializer;
      Class<?> clz = typeToken.getRawType();
      if (Modifier.isFinal(clz.getModifiers())) {
        clsExpr = new Literal(ctx.type(clz) + ".class");
        serializer = getOrCreateSerializer(clz);
      } else {
        clsExpr = new Invoke(classResolverRef, "readClass", "cls", CLASS_TYPE, false, buffer);
        serializer =
            new Invoke(classResolverRef, "getSerializer", TypeToken.of(Serializer.class), clsExpr);
        serializer = new Cast(serializer, TypeToken.of(MapSerializer.class), "mapSerializer");
      }
      Invoke supportHook =
          new Invoke(serializer, "supportCodegenHook", "supportHook", PRIMITIVE_BOOLEAN_TYPE);
      Expression size = new Invoke(buffer, "readInt", "size", PRIMITIVE_INT_TYPE);
      Expression newMap =
          new Invoke(serializer, "newMap", MAP_TYPE, furyRef, buffer, clsExpr, size);
      Expression start = new Literal(0, PRIMITIVE_INT_TYPE);
      Expression step = new Literal(1, PRIMITIVE_INT_TYPE);
      ForLoop readKeyValues =
          new ForLoop(
              start,
              size,
              step,
              i -> {
                Expression key = deserializeFor(buffer, keyType);
                Expression value = deserializeFor(buffer, valueType);
                return new ListExpression(key, value, new Invoke(newMap, "put", key, value));
              });
      // first newMap to create map, last newMap as expr value
      Expression hookRead = new ListExpression(size, newMap, readKeyValues, newMap);
      return new If(
          supportHook,
          hookRead,
          new Invoke(serializer, "read", MAP_TYPE, furyRef, buffer, clsExpr),
          false);
    }
  }
}
