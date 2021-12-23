package io.ray.serialization.encoder;

import static io.ray.serialization.codegen.Expression.Cast;
import static io.ray.serialization.codegen.Expression.FieldValue;
import static io.ray.serialization.codegen.Expression.Invoke;
import static io.ray.serialization.codegen.Expression.ListExpression;
import static io.ray.serialization.codegen.Expression.Literal;
import static io.ray.serialization.codegen.Expression.NewInstance;
import static io.ray.serialization.codegen.Expression.Reference;
import static io.ray.serialization.codegen.Expression.SetField;
import static io.ray.serialization.codegen.Expression.StaticInvoke;
import static io.ray.serialization.codegen.ExpressionUtils.literalStr;
import static io.ray.serialization.codegen.TypeUtils.CLASS_TYPE;
import static io.ray.serialization.codegen.TypeUtils.OBJECT_TYPE;
import static io.ray.serialization.codegen.TypeUtils.PRIMITIVE_VOID_TYPE;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import io.ray.serialization.Fury;
import io.ray.serialization.codegen.CodegenContext;
import io.ray.serialization.codegen.Expression;
import io.ray.serialization.serializers.Serializer;
import io.ray.serialization.serializers.UnsafeFieldAccessor;
import io.ray.serialization.types.TypeInference;
import io.ray.serialization.util.Descriptor;
import io.ray.serialization.util.MemoryBuffer;
import io.ray.serialization.util.ReflectionUtils;
import io.ray.serialization.util.StringUtils;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

/**
 * CodecBuilder is used for gen code for serialize java bean in row-format or sequential format.
 *
 * <h3>CodecBuilder has following requirements for the class of java bean:</h3>
 *
 * <ul>
 *   <li>public
 *   <li>class and super classes don't have a field that have the same name.
 *   <li>For instance inner class, ignore outer class field.
 *   <li>For instance inner class, deserialized outer class field is null
 * </ul>
 */
@SuppressWarnings("UnstableApiUsage")
public abstract class CodecBuilder {
  static final String ROOT_OBJECT_NAME = "obj";
  static final String FURY_NAME = "fury";

  static TypeToken<Object[]> objectArrayTypeToken = TypeToken.of(Object[].class);
  static TypeToken<MemoryBuffer> bufferTypeToken = TypeToken.of(MemoryBuffer.class);

  protected final CodegenContext ctx;
  protected final TypeToken<?> beanType;
  protected final Class<?> beanClass;
  protected Reference furyRef = new Reference(FURY_NAME, TypeToken.of(Fury.class));
  private Map<String, Reference> fieldMap = new HashMap<>();
  private Map<String, Reference> fieldAccessorMap = new HashMap<>();

  public CodecBuilder(CodegenContext ctx, TypeToken<?> beanType) {
    this.ctx = ctx;
    this.beanType = beanType;
    this.beanClass = beanType.getRawType();
    // don't ctx.addImport beanClass, because it maybe cause name collide.
    ctx.reserveName(ROOT_OBJECT_NAME);
    addCommonImports(ctx);

    Expression clsExpr;
    if (Modifier.isPublic(beanClass.getModifiers())) {
      clsExpr = new Literal(ctx.type(beanClass) + ".class");
    } else {
      // non-public class is not accessible in other class.
      clsExpr =
          new StaticInvoke(
              Class.class,
              "forName",
              CLASS_TYPE,
              false,
              new Literal(String.valueOf(beanClass.getCanonicalName())));
    }
    ctx.addField(Class.class, "beanClass", clsExpr);
  }

  private void addCommonImports(CodegenContext ctx) {
    TypeInference.SUPPORTED_TYPES.stream()
        .filter(
            typeToken ->
                !typeToken.isPrimitive()
                    && !typeToken.getRawType().getName().startsWith("java.lang"))
        .forEach(typeToken -> ctx.addImport((typeToken.getRawType().getCanonicalName())));
    ctx.addImport(Fury.class);
    ctx.addImport(MemoryBuffer.class.getPackage().getName() + ".*");
    ctx.addImport(Serializer.class.getPackage().getName() + ".*");
  }

  /** Generate codec class code */
  public abstract String genCode();

  /** Return an expression that serialize java bean of type {@link CodecBuilder#beanClass} */
  public abstract Expression buildEncodeExpression();

  // left null check in sub class encode method to reduce data dependence.
  private boolean fieldNullable = false;

  /** Return an expression that get field value from <code>bean</code>. */
  protected Expression getFieldValue(Expression inputBeanExpr, Descriptor descriptor) {
    TypeToken<?> fieldType = descriptor.getTypeToken();
    Preconditions.checkArgument(
        Modifier.isPublic(fieldType.getRawType().getModifiers()),
        "Field type should be public for codegen-based access");
    // public field or non-private non-java field access field directly.
    if (Modifier.isPublic(descriptor.getModifiers())) {
      return new FieldValue(inputBeanExpr, descriptor.getName(), fieldType, fieldNullable);
    } else if (descriptor.getReadMethod() != null
        && Modifier.isPublic(descriptor.getReadMethod().getModifiers())) {
      return new Invoke(
          inputBeanExpr,
          descriptor.getReadMethod().getName(),
          descriptor.getName(),
          fieldType,
          fieldNullable);
    } else {
      if (!Modifier.isPrivate(descriptor.getModifiers())) {
        if (AccessorHelper.defineAccessor(descriptor.getField())) {
          return new StaticInvoke(
              AccessorHelper.getAccessorClass(descriptor.getField()),
              descriptor.getName(),
              fieldType,
              fieldNullable,
              inputBeanExpr);
        }
      }
      if (descriptor.getReadMethod() != null
          && !Modifier.isPrivate(descriptor.getReadMethod().getModifiers())) {
        if (AccessorHelper.defineAccessor(descriptor.getReadMethod())) {
          return new StaticInvoke(
              AccessorHelper.getAccessorClass(descriptor.getReadMethod()),
              descriptor.getReadMethod().getName(),
              fieldType,
              fieldNullable,
              inputBeanExpr);
        }
      }
      return unsafeAccessField(inputBeanExpr, beanClass, descriptor);
    }
  }

  /** Return an expression that get field value> from <code>bean</code> using reflection */
  private Expression reflectAccessField(
      Expression inputObject, Class<?> cls, Descriptor descriptor) {
    Reference fieldRef = getOrCreateField(cls, descriptor.getName());
    // boolean fieldNullable = !descriptor.getTypeToken().isPrimitive();
    Invoke getObj = new Invoke(fieldRef, "get", OBJECT_TYPE, fieldNullable, inputObject);
    return new Cast(getObj, descriptor.getTypeToken(), descriptor.getName());
  }

  /**
   * Return an expression that get field value> from <code>bean</code> using {@link
   * UnsafeFieldAccessor}
   */
  private Expression unsafeAccessField(
      Expression inputObject, Class<?> cls, Descriptor descriptor) {
    Reference fieldAccessorRef = getOrCreateFieldAccessor(cls, descriptor.getName());
    if (descriptor.getTypeToken().isPrimitive()) {
      TypeToken returnType = descriptor.getTypeToken();
      String funcName = "get" + StringUtils.capitalize(returnType.getRawType().toString());
      return new Invoke(fieldAccessorRef, funcName, returnType, fieldNullable, inputObject);
    } else {
      Invoke getObj =
          new Invoke(fieldAccessorRef, "getObject", OBJECT_TYPE, fieldNullable, inputObject);
      return new Cast(getObj, descriptor.getTypeToken(), descriptor.getName());
    }
  }

  /**
   * Return an expression that deserialize data as a java bean of type {@link
   * CodecBuilder#beanClass}
   */
  public abstract Expression buildDecodeExpression();

  /** Return an expression that set field <code>value</code> to <code>bean</code>. */
  protected Expression setFieldValue(Expression bean, Descriptor d, Expression value) {
    String fieldName = d.getName();
    if (!Modifier.isFinal(d.getModifiers()) && Modifier.isPublic(d.getModifiers())) {
      return new SetField(bean, fieldName, value);
    } else if (d.getWriteMethod() != null && Modifier.isPublic(d.getWriteMethod().getModifiers())) {
      return new Invoke(bean, d.getWriteMethod().getName(), value);
    } else {
      if (!Modifier.isFinal(d.getModifiers()) && !Modifier.isPrivate(d.getModifiers())) {
        if (AccessorHelper.defineAccessor(d.getField())) {
          Class<?> accessorClass = AccessorHelper.getAccessorClass(d.getField());
          return new StaticInvoke(
              accessorClass, d.getName(), PRIMITIVE_VOID_TYPE, false, bean, value);
        }
      }
      if (d.getWriteMethod() != null && !Modifier.isPrivate(d.getWriteMethod().getModifiers())) {
        if (AccessorHelper.defineAccessor(d.getWriteMethod())) {
          Class<?> accessorClass = AccessorHelper.getAccessorClass(d.getWriteMethod());
          return new StaticInvoke(
              accessorClass, d.getWriteMethod().getName(), PRIMITIVE_VOID_TYPE, false, bean, value);
        }
      }
      return unsafeSetField(bean, d, value);
    }
  }

  /**
   * Return an expression that set field <code>value</code> to <code>bean</code> using reflection
   */
  private Expression reflectSetField(Expression bean, String fieldName, Expression value) {
    // Class maybe have getter, but don't have setter, so we can't rely on reflectAccessField to
    // populate fieldMap
    Reference fieldRef = getOrCreateField(bean.type().getRawType(), fieldName);
    Preconditions.checkNotNull(fieldRef);
    return new Invoke(fieldRef, "set", bean, value);
  }

  /**
   * Return an expression that set field <code>value</code> to <code>bean</code> using {@link
   * UnsafeFieldAccessor}
   */
  private Expression unsafeSetField(Expression bean, Descriptor descriptor, Expression value) {
    Reference fieldAccessorRef =
        getOrCreateFieldAccessor(bean.type().getRawType(), descriptor.getName());
    if (descriptor.getTypeToken().isPrimitive()) {
      TypeToken fieldType = descriptor.getTypeToken();
      Preconditions.checkArgument(value.type().equals(fieldType));
      String funcName = "put" + StringUtils.capitalize(fieldType.getRawType().toString());
      return new Invoke(fieldAccessorRef, funcName, bean, value);
    } else {
      return new Invoke(fieldAccessorRef, "putObject", bean, value);
    }
  }

  private Reference getOrCreateField(Class<?> cls, String fieldName) {
    Reference fieldRef = fieldMap.get(fieldName);
    if (fieldRef == null) {
      TypeToken<Field> fieldTypeToken = TypeToken.of(Field.class);
      String fieldRefName = ctx.freshName(fieldName + "Field");
      Preconditions.checkArgument(Modifier.isPublic(cls.getModifiers()));
      Literal clzLiteral = new Literal(ctx.type(cls) + ".class");
      StaticInvoke fieldExpr =
          new StaticInvoke(
              ReflectionUtils.class,
              "getField",
              fieldTypeToken,
              false,
              clzLiteral,
              literalStr(fieldName));
      Invoke setAccessible = new Invoke(fieldExpr, "setAccessible", new Literal("true"));
      ListExpression createField = new ListExpression(setAccessible, fieldExpr);
      ctx.addField(ctx.type(Field.class), fieldRefName, createField);
      fieldRef = new Reference(fieldRefName, fieldTypeToken);
      fieldMap.put(fieldName, fieldRef);
    }
    return fieldRef;
  }

  private Reference getOrCreateFieldAccessor(Class<?> cls, String fieldName) {
    Reference fieldAccessorRef = fieldAccessorMap.get(fieldName);
    if (fieldAccessorRef == null) {
      String fieldAccessorRefName = ctx.freshName(fieldName + "Accessor");
      Literal clzLiteral = new Literal(ctx.type(cls) + ".class");
      NewInstance createFieldAccessor =
          new NewInstance(
              TypeToken.of(UnsafeFieldAccessor.class), clzLiteral, literalStr(fieldName));
      ctx.addField(ctx.type(UnsafeFieldAccessor.class), fieldAccessorRefName, createFieldAccessor);
      fieldAccessorRef =
          new Reference(fieldAccessorRefName, TypeToken.of(UnsafeFieldAccessor.class));
      fieldAccessorMap.put(fieldName, fieldAccessorRef);
    }
    return fieldAccessorRef;
  }

  /** @return an Expression that create a new java object of type {@link CodecBuilder#beanClass} */
  protected Expression newBean() {
    Preconditions.checkArgument(Modifier.isPublic(beanClass.getModifiers()));
    return new NewInstance(beanType);
  }
}
