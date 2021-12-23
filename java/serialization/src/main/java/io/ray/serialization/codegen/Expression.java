package io.ray.serialization.codegen;

import static io.ray.serialization.codegen.Code.ExprCode;
import static io.ray.serialization.codegen.Code.LiteralValue;
import static io.ray.serialization.codegen.Code.LiteralValue.FalseLiteral;
import static io.ray.serialization.codegen.Code.LiteralValue.TrueLiteral;
import static io.ray.serialization.codegen.Code.literal;
import static io.ray.serialization.codegen.CodeGenerator.alignIndent;
import static io.ray.serialization.codegen.CodeGenerator.appendNewlineIfNeeded;
import static io.ray.serialization.codegen.CodeGenerator.stripIfHasLastNewline;
import static io.ray.serialization.codegen.ExpressionUtils.callFunc;
import static io.ray.serialization.util.StringUtils.capitalize;
import static io.ray.serialization.util.StringUtils.format;
import static io.ray.serialization.util.StringUtils.isBlank;
import static io.ray.serialization.util.StringUtils.isNotBlank;
import static io.ray.serialization.util.StringUtils.stripBlankLines;
import static io.ray.serialization.util.TypeUtils.BOOLEAN_TYPE;
import static io.ray.serialization.util.TypeUtils.ITERABLE_TYPE;
import static io.ray.serialization.util.TypeUtils.OBJECT_TYPE;
import static io.ray.serialization.util.TypeUtils.PRIMITIVE_BOOLEAN_TYPE;
import static io.ray.serialization.util.TypeUtils.PRIMITIVE_VOID_TYPE;
import static io.ray.serialization.util.TypeUtils.STRING_TYPE;
import static io.ray.serialization.util.TypeUtils.boxedType;
import static io.ray.serialization.util.TypeUtils.defaultValue;
import static io.ray.serialization.util.TypeUtils.getArrayType;
import static io.ray.serialization.util.TypeUtils.getElementType;
import static io.ray.serialization.util.TypeUtils.isPrimitive;
import static io.ray.serialization.util.TypeUtils.maxType;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import io.ray.serialization.util.Platform;
import io.ray.serialization.util.ReflectionUtils;
import java.lang.reflect.Array;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An expression represents a piece of code evaluation logic which can be generated to valid java
 * code. Expression can be used to compose complex code logic.
 */
@SuppressWarnings("UnstableApiUsage")
public interface Expression {

  /**
   * Returns the Class<?> of the result of evaluating this expression. It is invalid to query the
   * type of an unresolved expression (i.e., when `resolved` == false).
   */
  TypeToken<?> type();

  /**
   * If expression is already generated in this context, returned exprCode won't contains code, so
   * we can reuse/elimination expression code.
   */
  default ExprCode genCode(CodegenContext ctx) {
    // Ctx already contains expression code, which means that the code to evaluate it has already
    // been added before. In that case, we just reuse it.
    ExprCode reuseExprCode = ctx.exprState.get(this);
    if (reuseExprCode != null) {
      return reuseExprCode;
    } else {
      ExprCode genCode = doGenCode(ctx);
      ctx.exprState.put(this, new ExprCode(genCode.isNull(), genCode.value()));
      return genCode;
    }
  }

  /**
   * Used when Expression is requested to doGenCode.
   *
   * @param ctx a [[CodegenContext]]
   * @return an [[ExprCode]] containing the Java source code to generate the given expression
   */
  ExprCode doGenCode(CodegenContext ctx);

  default boolean nullable() {
    return false;
  }

  // ###########################################################
  // ####################### Expressions #######################
  // ###########################################################

  /** An expression that have a value as the result of the evaluation */
  abstract class ValueExpression implements Expression {
    // set to others to get a more context-dependent variable name.
    public String valuePrefix = "value";

    public String isNullPrefix() {
      return "is" + capitalize(valuePrefix) + "Null";
    }
  }

  /**
   * A ListExpression is a list of expressions. Use last expression's type/nullable/value/IsNull to
   * represent ListExpression's type/nullable/value/IsNull
   */
  class ListExpression implements Expression {
    private final List<Expression> expressions;
    private Expression last;

    public ListExpression(Expression... expressions) {
      this(new ArrayList<>(Arrays.asList(expressions)));
    }

    public ListExpression(List<Expression> expressions) {
      this.expressions = expressions;
      if (!this.expressions.isEmpty()) {
        this.last = this.expressions.get(this.expressions.size() - 1);
      }
    }

    @Override
    public TypeToken<?> type() {
      Preconditions.checkNotNull(last);
      return last.type();
    }

    @Override
    public ExprCode doGenCode(CodegenContext ctx) {
      StringBuilder codeBuilder = new StringBuilder();
      boolean hasCode = false;
      for (Expression expr : expressions) {
        ExprCode code = expr.genCode(ctx);
        if (isNotBlank(code.code())) {
          appendNewlineIfNeeded(codeBuilder);
          codeBuilder.append(code.code());
          hasCode = true;
        }
      }
      ExprCode lastExprCode = last.genCode(ctx);
      String code = codeBuilder.toString();
      if (!hasCode) {
        code = null;
      }
      return new ExprCode(code, lastExprCode.isNull(), lastExprCode.value());
    }

    @Override
    public boolean nullable() {
      return last.nullable();
    }

    public List<Expression> expressions() {
      return expressions;
    }

    public ListExpression add(Expression expr) {
      Preconditions.checkNotNull(expr);
      this.expressions.add(expr);
      this.last = expr;
      return this;
    }

    public ListExpression add(Expression expr, Expression... exprs) {
      add(expr);
      return addAll(Arrays.asList(exprs));
    }

    public ListExpression addAll(List<Expression> exprs) {
      Preconditions.checkNotNull(exprs);
      this.expressions.addAll(exprs);
      if (!exprs.isEmpty()) {
        this.last = exprs.get(exprs.size() - 1);
      }
      return this;
    }

    @Override
    public String toString() {
      return expressions.stream().map(Object::toString).collect(Collectors.joining(","));
    }
  }

  class Literal implements Expression {
    private Object value;
    private TypeToken<?> type;

    public Literal(String value) {
      this.value = value;
      this.type = STRING_TYPE;
    }

    public Literal(Object value, TypeToken<?> type) {
      this.value = value;
      this.type = type;
    }

    @Override
    public TypeToken<?> type() {
      return type;
    }

    @Override
    public ExprCode doGenCode(CodegenContext ctx) {
      Class<?> javaType = type.getRawType();
      if (isPrimitive(javaType)) {
        javaType = boxedType(javaType);
      }
      if (value == null) {
        LiteralValue defaultLiteral = literal(javaType, defaultValue(javaType));
        return new ExprCode(null, TrueLiteral, defaultLiteral);
      } else {
        if (javaType == String.class) {
          return new ExprCode(FalseLiteral, literal(value));
        } else if (javaType == Boolean.class || javaType == Integer.class) {
          return new ExprCode(null, FalseLiteral, literal(javaType, value.toString()));
        } else if (javaType == Float.class) {
          Float f = (Float) value;
          if (f.isNaN()) {
            return new ExprCode(FalseLiteral, literal(javaType, "Float.NaN"));
          } else if (f.equals(Float.POSITIVE_INFINITY)) {
            return new ExprCode(FalseLiteral, literal(javaType, "Float.POSITIVE_INFINITY"));
          } else if (f.equals(Float.NEGATIVE_INFINITY)) {
            return new ExprCode(FalseLiteral, literal(javaType, "Float.NEGATIVE_INFINITY"));
          } else {
            return new ExprCode(FalseLiteral, literal(javaType, String.format("%fF", f)));
          }
        } else if (javaType == Double.class) {
          Double d = (Double) value;
          if (d.isNaN()) {
            return new ExprCode(FalseLiteral, literal(javaType, "Double.NaN"));
          } else if (d.equals(Double.POSITIVE_INFINITY)) {
            return new ExprCode(FalseLiteral, literal(javaType, "Double.POSITIVE_INFINITY"));
          } else if (d.equals(Double.NEGATIVE_INFINITY)) {
            return new ExprCode(FalseLiteral, literal(javaType, "Double.NEGATIVE_INFINITY"));
          } else {
            return new ExprCode(FalseLiteral, literal(javaType, String.format("%fD", d)));
          }
        } else if (javaType == Byte.class) {
          return new ExprCode(
              FalseLiteral, Code.exprValue(javaType, String.format("(%s)%s", "byte", value)));
        } else if (javaType == Short.class) {
          return new ExprCode(
              FalseLiteral, Code.exprValue(javaType, String.format("(%s)%s", "short", value)));
        } else if (javaType == Long.class) {
          return new ExprCode(FalseLiteral, literal(javaType, String.format("%dL", (Long) value)));
        } else if (isPrimitive(javaType)) {
          return new ExprCode(FalseLiteral, literal(javaType, String.valueOf(value)));
        } else {
          throw new UnsupportedOperationException("Unsupported type " + javaType);
        }
      }
    }

    @Override
    public String toString() {
      if (value == null) {
        return "null";
      } else {
        return value.toString();
      }
    }
  }

  /**
   * A Reference is a variable/field that can be can be accessed in the expression's CodegenContext
   */
  class Reference implements Expression {
    private String name;
    private TypeToken<?> type;
    private boolean nullable;

    public Reference(String name, TypeToken<?> type) {
      this.name = name;
      this.type = type;
      this.nullable = false;
    }

    public Reference(String name, TypeToken<?> type, boolean nullable) {
      this.name = name;
      this.type = type;
      this.nullable = nullable;
    }

    @Override
    public TypeToken<?> type() {
      return type;
    }

    @Override
    public ExprCode doGenCode(CodegenContext ctx) {
      if (nullable) {
        String isNull = ctx.freshName("isNull");
        String code =
            format("boolean ${isNull} = ${name} == null;", "isNull", isNull, "name", name);
        return new ExprCode(
            code, Code.isNullVariable(isNull), Code.variable(type.getRawType(), name));
      } else {
        return new ExprCode(FalseLiteral, Code.variable(type.getRawType(), name));
      }
    }

    @Override
    public boolean nullable() {
      return nullable;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  class FieldValue implements Expression {
    private Expression targetObject;
    private String fieldName;
    private TypeToken<?> type;
    private boolean fieldNullable;

    public FieldValue(Expression targetObject, String fieldName, TypeToken<?> type) {
      this(targetObject, fieldName, type, !type.isPrimitive());
    }

    public FieldValue(
        Expression targetObject, String fieldName, TypeToken<?> type, boolean fieldNullable) {
      this.targetObject = targetObject;
      this.fieldName = fieldName;
      this.type = type;
      this.fieldNullable = fieldNullable;
      Preconditions.checkArgument(type != null);
    }

    @Override
    public TypeToken<?> type() {
      return type;
    }

    @Override
    public ExprCode doGenCode(CodegenContext ctx) {
      StringBuilder codeBuilder = new StringBuilder();
      ExprCode targetExprCode = targetObject.genCode(ctx);
      if (isNotBlank(targetExprCode.code())) {
        codeBuilder.append(targetExprCode.code()).append("\n");
      }

      Class<?> rawType = type.getRawType();
      // although isNull is not always used, we place it outside and get freshNames simultaneously
      // to have
      // same suffix, thus get more readability.
      String[] freshNames = ctx.freshNames(fieldName, "is" + capitalize(fieldName) + "Null");
      String value = freshNames[0];
      String isNull = freshNames[1];
      if (fieldNullable) {
        codeBuilder.append(String.format("boolean %s = false;\n", isNull));
        String code =
            format(
                "${type} ${value} = ${target}.${fieldName};\n",
                "type",
                ctx.type(type),
                "value",
                value,
                "defaultValue",
                defaultValue(rawType),
                "target",
                targetExprCode.value(),
                "fieldName",
                fieldName);
        codeBuilder.append(code);

        String nullCode =
            format(
                "if (${value} == null) {\n" + "    ${isNull} = true;\n" + "}",
                "value",
                value,
                "isNull",
                isNull);
        codeBuilder.append(nullCode);
        return new ExprCode(
            codeBuilder.toString(), Code.isNullVariable(isNull), Code.variable(rawType, value));
      } else {
        String code =
            format(
                "${type} ${value} = ${target}.${fieldName};",
                "type",
                type.getRawType().getCanonicalName(),
                "value",
                value,
                "target",
                targetExprCode.value(),
                "fieldName",
                fieldName);
        codeBuilder.append(code);
        return new ExprCode(
            codeBuilder.toString(), FalseLiteral, Code.variable(type.getRawType(), value));
      }
    }

    @Override
    public boolean nullable() {
      return fieldNullable;
    }

    @Override
    public String toString() {
      return String.format("(%s).%s", targetObject, fieldName);
    }
  }

  class SetField implements Expression {
    private final Expression targetObject;
    private final String fieldName;
    private final Expression fieldValue;

    public SetField(Expression targetObject, String fieldName, Expression fieldValue) {
      this.targetObject = targetObject;
      this.fieldName = fieldName;
      this.fieldValue = fieldValue;
    }

    @Override
    public TypeToken<?> type() {
      return PRIMITIVE_VOID_TYPE;
    }

    @Override
    public ExprCode doGenCode(CodegenContext ctx) {
      StringBuilder codeBuilder = new StringBuilder();
      ExprCode targetExprCode = targetObject.genCode(ctx);
      ExprCode fieldValueExprCode = fieldValue.genCode(ctx);
      Stream.of(targetExprCode, fieldValueExprCode)
          .forEach(
              exprCode -> {
                if (isNotBlank(exprCode.code())) {
                  codeBuilder.append(exprCode.code()).append('\n');
                }
              });
      // don't check whether ${target} null, place it in if expression
      String assign =
          format(
              "${target}.${fieldName} = ${fieldValue};",
              "target",
              targetExprCode.value(),
              "fieldName",
              fieldName,
              "fieldValue",
              fieldValueExprCode.value());
      codeBuilder.append(assign);
      return new ExprCode(codeBuilder.toString(), null, null);
    }

    public String toString() {
      return String.format("SetField(%s, %s, %s)", targetObject, fieldName, fieldValue);
    }
  }

  class Cast implements Expression {
    private final Expression targetObject;
    private final String castedValueNamePrefix;
    private TypeToken<?> type;

    public Cast(Expression targetObject, TypeToken<?> type) {
      this(targetObject, type, "castedValue");
    }

    public Cast(Expression targetObject, TypeToken<?> type, String castedValueNamePrefix) {
      this.targetObject = targetObject;
      this.type = type;
      this.castedValueNamePrefix = castedValueNamePrefix;
    }

    @Override
    public TypeToken<?> type() {
      return type;
    }

    @Override
    public ExprCode doGenCode(CodegenContext ctx) {
      StringBuilder codeBuilder = new StringBuilder();
      Class<?> rawType = type.getRawType();
      ExprCode targetExprCode = targetObject.genCode(ctx);
      if (isNotBlank(targetExprCode.code())) {
        codeBuilder.append(targetExprCode.code());
        codeBuilder.append("\n");
      }
      // workaround: Cannot cast "java.lang.Object" to "long". then use java auto box/unbox.
      String withCast = ctx.type(type);
      if (type.isPrimitive() && !targetObject.type().isPrimitive()) {
        withCast = ctx.type(boxedType(type.getRawType()));
      }
      String castedValue = ctx.freshName(castedValueNamePrefix);
      String cast =
          format(
              "${type} ${castedValue} = (${withCast})${target};",
              "type",
              ctx.type(type),
              "withCast",
              withCast,
              "castedValue",
              castedValue,
              "target",
              targetExprCode.value());
      codeBuilder.append(cast);
      return new ExprCode(
          codeBuilder.toString(), targetExprCode.isNull(), Code.variable(rawType, castedValue));
    }

    @Override
    public boolean nullable() {
      return targetObject.nullable();
    }

    @Override
    public String toString() {
      return String.format("(%s)%s", type, targetObject);
    }
  }

  class Invoke implements Expression {
    private final Expression targetObject;
    private final String functionName;
    private final String returnNamePrefix;
    private final TypeToken<?> type;
    private final Expression[] arguments;
    private final boolean returnNullable;

    /** Invoke don't return value, this is a procedure call for side effect */
    public Invoke(Expression targetObject, String functionName, Expression... arguments) {
      this(targetObject, functionName, PRIMITIVE_VOID_TYPE, false, arguments);
    }

    /** Invoke don't accept arguments */
    public Invoke(Expression targetObject, String functionName, TypeToken<?> type) {
      this(targetObject, functionName, type, false);
    }

    /** Invoke don't accept arguments */
    public Invoke(
        Expression targetObject, String functionName, String returnNamePrefix, TypeToken<?> type) {
      this(targetObject, functionName, returnNamePrefix, type, false);
    }

    public Invoke(
        Expression targetObject, String functionName, TypeToken<?> type, Expression... arguments) {
      this(targetObject, functionName, "value", type, false, arguments);
    }

    public Invoke(
        Expression targetObject,
        String functionName,
        TypeToken<?> type,
        boolean returnNullable,
        Expression... arguments) {
      this(targetObject, functionName, "value", type, returnNullable, arguments);
    }

    public Invoke(
        Expression targetObject,
        String functionName,
        String returnNamePrefix,
        TypeToken<?> type,
        boolean returnNullable,
        Expression... arguments) {
      this.targetObject = targetObject;
      this.functionName = functionName;
      this.returnNamePrefix = returnNamePrefix;
      this.type = type;
      this.returnNullable = returnNullable;
      this.arguments = arguments;
    }

    @Override
    public TypeToken<?> type() {
      return type;
    }

    @Override
    public ExprCode doGenCode(CodegenContext ctx) {
      StringBuilder codeBuilder = new StringBuilder();
      ExprCode targetExprCode = targetObject.genCode(ctx);
      if (isNotBlank(targetExprCode.code())) {
        codeBuilder.append(targetExprCode.code());
        codeBuilder.append("\n");
      }
      int len = arguments.length;
      StringBuilder argsBuilder = new StringBuilder();
      if (len > 0) {
        for (int i = 0; i < len; i++) {
          Expression argExpr = arguments[i];
          ExprCode argExprCode = argExpr.genCode(ctx);
          if (isNotBlank(argExprCode.code())) {
            codeBuilder.append(argExprCode.code()).append("\n");
          }
          if (i != 0) {
            argsBuilder.append(", ");
          }
          argsBuilder.append(argExprCode.value());
        }
      }

      boolean needTryCatch =
          ReflectionUtils.hasException(targetObject.type().getRawType(), functionName);
      if (type != null && !PRIMITIVE_VOID_TYPE.equals(type)) {
        Class<?> rawType = type.getRawType();
        String[] freshNames = ctx.freshNames(returnNamePrefix, returnNamePrefix + "IsNull");
        String value = freshNames[0];
        String isNull = freshNames[1];
        if (returnNullable) {
          codeBuilder.append(String.format("boolean %s = false;\n", isNull));
          String callCode =
              callFunc(
                  ctx.type(type),
                  value,
                  targetExprCode.value().code(),
                  functionName,
                  argsBuilder.toString(),
                  needTryCatch);
          codeBuilder.append(callCode).append('\n');
        } else {
          String callCode =
              callFunc(
                  ctx.type(type),
                  value,
                  targetExprCode.value().code(),
                  functionName,
                  argsBuilder.toString(),
                  needTryCatch);
          codeBuilder.append(callCode);
        }

        if (returnNullable) {
          String nullCode =
              format(
                  "if (${value} == null) {\n" + "    ${isNull} = true;\n" + "}",
                  "value",
                  value,
                  "isNull",
                  isNull);
          codeBuilder.append(nullCode);
          return new ExprCode(
              codeBuilder.toString(), Code.isNullVariable(isNull), Code.variable(rawType, value));
        } else {
          return new ExprCode(codeBuilder.toString(), FalseLiteral, Code.variable(rawType, value));
        }
      } else {
        String call =
            callFunc(
                targetExprCode.value().code(), functionName, argsBuilder.toString(), needTryCatch);
        codeBuilder.append(call);
        return new ExprCode(codeBuilder.toString(), null, null);
      }
    }

    @Override
    public boolean nullable() {
      return returnNullable;
    }

    @Override
    public String toString() {
      return String.format("%s.%s", targetObject, functionName);
    }
  }

  class StaticInvoke implements Expression {
    private Class<?> staticObject;
    private String functionName;
    private String returnNamePrefix;
    private TypeToken<?> type;
    private Expression[] arguments;
    private boolean returnNullable;

    public StaticInvoke(Class<?> staticObject, String functionName, TypeToken<?> type) {
      this(staticObject, functionName, type, false);
    }

    public StaticInvoke(
        Class<?> staticObject,
        String functionName,
        TypeToken<?> type,
        boolean returnNullable,
        Expression... arguments) {
      this(staticObject, functionName, "value", type, returnNullable, arguments);
    }

    /**
     * @param staticObject The target of the static call
     * @param functionName The name of the method to call
     * @param returnNamePrefix returnNamePrefix
     * @param type return type of the function call
     * @param returnNullable When false, indicating the invoked method will always return non-null
     *     value.
     * @param arguments An optional list of expressions to pass as arguments to the function
     */
    public StaticInvoke(
        Class<?> staticObject,
        String functionName,
        String returnNamePrefix,
        TypeToken<?> type,
        boolean returnNullable,
        Expression... arguments) {
      this.staticObject = staticObject;
      this.functionName = functionName;
      this.type = type;
      this.arguments = arguments;
      this.returnNullable = returnNullable;
      this.returnNamePrefix = returnNamePrefix;
    }

    @Override
    public TypeToken<?> type() {
      return type;
    }

    @Override
    public ExprCode doGenCode(CodegenContext ctx) {
      StringBuilder codeBuilder = new StringBuilder();
      int len = arguments.length;
      StringBuilder argsBuilder = new StringBuilder();
      if (len > 0) {
        for (int i = 0; i < len; i++) {
          Expression argExpr = arguments[i];
          ExprCode argExprCode = argExpr.genCode(ctx);
          if (isNotBlank(argExprCode.code())) {
            codeBuilder.append(argExprCode.code()).append("\n");
          }
          if (i != 0) {
            argsBuilder.append(", ");
          }
          argsBuilder.append(argExprCode.value());
        }
      }

      boolean needTryCatch = ReflectionUtils.hasException(staticObject, functionName);
      if (type != null && !PRIMITIVE_VOID_TYPE.equals(type)) {
        Class<?> rawType = type.getRawType();
        String[] freshNames = ctx.freshNames(returnNamePrefix, "isNull");
        String value = freshNames[0];
        String isNull = freshNames[1];
        if (returnNullable) {
          codeBuilder.append(String.format("boolean %s = false;\n", isNull));
          String callCode =
              callFunc(
                  ctx.type(type),
                  value,
                  ctx.type(staticObject),
                  functionName,
                  argsBuilder.toString(),
                  needTryCatch);
          codeBuilder.append(callCode).append('\n');
        } else {
          String callCode =
              callFunc(
                  ctx.type(type),
                  value,
                  ctx.type(staticObject),
                  functionName,
                  argsBuilder.toString(),
                  needTryCatch);
          codeBuilder.append(callCode);
        }

        if (returnNullable) {
          String nullCode =
              format(
                  "if (${value} == null) {\n" + "   ${isNull} = true;\n" + "}",
                  "value",
                  value,
                  "isNull",
                  isNull);
          codeBuilder.append(nullCode);
          return new ExprCode(
              codeBuilder.toString(), Code.isNullVariable(isNull), Code.variable(rawType, value));
        } else {
          return new ExprCode(codeBuilder.toString(), FalseLiteral, Code.variable(rawType, value));
        }
      } else {
        String call =
            callFunc(ctx.type(staticObject), functionName, argsBuilder.toString(), needTryCatch);
        codeBuilder.append(call);
        return new ExprCode(codeBuilder.toString(), null, null);
      }
    }

    @Override
    public boolean nullable() {
      return returnNullable;
    }

    @Override
    public String toString() {
      return String.format("%s.%s", staticObject, functionName);
    }
  }

  class NewInstance implements Expression {
    private TypeToken<?> type;
    private String unknownClassName;
    private List<Expression> arguments;
    private Expression outerPointer;
    private final boolean needOuterPointer;

    /**
     * @param interfaceType object declared in this type. actually the type is {@code
     *     unknownClassName}
     * @param unknownClassName unknownClassName that's unknown in compile-time
     */
    public NewInstance(
        TypeToken<?> interfaceType, String unknownClassName, Expression... arguments) {
      this(interfaceType, Arrays.asList(arguments), null);
      this.unknownClassName = unknownClassName;
      check();
    }

    public NewInstance(TypeToken<?> type, Expression... arguments) {
      this(type, Arrays.asList(arguments), null);
      check();
    }

    private NewInstance(TypeToken<?> type, List<Expression> arguments, Expression outerPointer) {
      this.type = type;
      this.outerPointer = outerPointer;
      this.arguments = arguments;
      this.needOuterPointer =
          type.getRawType().isMemberClass() && !Modifier.isStatic(type.getRawType().getModifiers());
      if (needOuterPointer && (outerPointer == null)) {
        String msg =
            String.format("outerPointer can't be null when %s is instance inner class", type);
        throw new CodegenException(msg);
      }
    }

    private void check() {
      Preconditions.checkArgument(
          !type.isArray(), "Please use " + NewArray.class + " to create array.");
      if (unknownClassName == null && arguments.size() > 0) {
        // If unknownClassName is not null, we don't have actual type object,
        // we assume we can create instance of unknownClassName.
        // If arguments size is 0, we can always create instance of class, even by
        // unsafe.allocateInstance
        boolean anyMatchParamCount =
            Stream.of(type.getRawType().getConstructors())
                .anyMatch(c -> c.getParameterCount() == arguments.size());
        if (!anyMatchParamCount) {
          String msg =
              String.format(
                  "%s doesn't have a public constructor that take %d params",
                  type, arguments.size());
          throw new IllegalArgumentException(msg);
        }
      }
    }

    @Override
    public TypeToken<?> type() {
      return type;
    }

    @Override
    public ExprCode doGenCode(CodegenContext ctx) {
      StringBuilder codeBuilder = new StringBuilder();
      int len = arguments.size();
      StringBuilder argsBuilder = new StringBuilder();
      if (len > 0) {
        for (int i = 0; i < len; i++) {
          Expression argExpr = arguments.get(i);
          ExprCode argExprCode = argExpr.genCode(ctx);
          if (isNotBlank(argExprCode.code())) {
            codeBuilder.append(argExprCode.code()).append("\n");
          }
          if (i != 0) {
            argsBuilder.append(", ");
          }
          argsBuilder.append(argExprCode.value());
        }
      }

      Class<?> rawType = type.getRawType();
      String type = ctx.type(rawType);
      String clzName = unknownClassName;
      if (clzName == null) {
        clzName = type;
      }
      if (needOuterPointer) {
        // "${gen.value}.new ${cls.getSimpleName}($argString)"
        throw new UnsupportedOperationException();
      } else {
        String value = ctx.freshName(rawType);
        // class don't have a public no-arg constructor
        if (arguments.isEmpty() && !ReflectionUtils.hasPublicNoArgConstructor(rawType)) {
          // janino doesn't generics, so we cast manually.
          String instance = ctx.freshName("instance");
          String code =
              callFunc(
                  "Object",
                  instance,
                  ctx.type(Platform.class),
                  "newInstance",
                  clzName + ".class",
                  false);
          codeBuilder.append(code).append('\n');
          String cast =
              format(
                  "${clzName} ${value} = (${clzName})${instance};",
                  "clzName",
                  clzName,
                  "value",
                  value,
                  "instance",
                  instance);
          codeBuilder.append(cast);
        } else {
          String code =
              format(
                  "${clzName} ${value} = new ${clzName}(${args});",
                  "clzName",
                  clzName,
                  "value",
                  value,
                  "args",
                  argsBuilder.toString());
          codeBuilder.append(code);
        }
        return new ExprCode(codeBuilder.toString(), null, Code.variable(rawType, value));
      }
    }

    @Override
    public boolean nullable() {
      return false;
    }

    @Override
    public String toString() {
      return String.format("newInstance(%s)", type);
    }
  }

  class NewArray implements Expression {
    private TypeToken<?> type;
    private Expression[] elements;

    private int numDimensions;
    private Class<?> elemType;

    private Expression dim;

    private Expression dims;

    public NewArray(Class<?> elemType, Expression dim) {
      this.numDimensions = 1;
      this.elemType = elemType;
      this.dim = dim;
      type = TypeToken.of(Array.newInstance(elemType, 1).getClass());
    }

    /**
     * dynamic created array doesn't have generic type info, so we don't pass in TypeToken
     *
     * @param elemType elemType
     * @param numDimensions numDimensions
     * @param dims an int[] represent dims
     */
    public NewArray(Class<?> elemType, int numDimensions, Expression dims) {
      this.numDimensions = numDimensions;
      this.elemType = elemType;
      this.dims = dims;

      int[] stubSizes = new int[numDimensions];
      for (int i = 0; i < numDimensions; i++) {
        stubSizes[i] = 1;
      }
      type = TypeToken.of(Array.newInstance(elemType, stubSizes).getClass());
    }

    public NewArray(TypeToken<?> type, Expression... elements) {
      this.type = type;
      this.elements = elements;
      this.numDimensions = 1;
    }

    /** ex: new int[3][][] */
    public static NewArray newArrayWithFirstDim(
        Class<?> elemType, int numDimensions, Expression firstDim) {
      NewArray array = new NewArray(elemType, firstDim);
      array.numDimensions = numDimensions;
      return array;
    }

    @Override
    public TypeToken<?> type() {
      return type;
    }

    @Override
    public ExprCode doGenCode(CodegenContext ctx) {
      StringBuilder codeBuilder = new StringBuilder();
      Class<?> rawType = type.getRawType();
      String arrayType = getArrayType(rawType);
      String value = ctx.freshName("arr");
      if (dims != null) {
        // multi-dimension array
        ExprCode dimsExprCode = dims.genCode(ctx);
        if (isNotBlank(dimsExprCode.code())) {
          codeBuilder.append(dimsExprCode.code()).append('\n');
        }
        // "${arrType} ${value} = new ${elementType}[$?][$?]...
        codeBuilder
            .append(arrayType)
            .append(' ')
            .append(value)
            .append(" = new ")
            .append(ctx.type(elemType));
        for (int i = 0; i < numDimensions; i++) {
          // dims is dimensions array, which store size of per dim.
          String iDim = format("${dims}[${i}]", "dims", dimsExprCode.value(), "i", i);
          codeBuilder.append('[').append(iDim).append("]");
        }
        codeBuilder.append(';');
      } else if (dim != null) {
        ExprCode dimExprCode = dim.genCode(ctx);
        if (isNotBlank(dimExprCode.code())) {
          codeBuilder.append(dimExprCode.code()).append('\n');
        }
        if (numDimensions > 1) {
          // multi-dimension array
          // "${arrType} ${value} = new ${elementType}[$?][][][]...
          codeBuilder
              .append(arrayType)
              .append(' ')
              .append(value)
              .append(" = new ")
              .append(ctx.type(elemType));
          codeBuilder.append('[').append(dimExprCode.value()).append(']');
          for (int i = 1; i < numDimensions; i++) {
            codeBuilder.append('[').append("]");
          }
          codeBuilder.append(';');
        } else {
          // one-dimension array
          String code =
              format(
                  "${type} ${value} = new ${elemType}[${dim}];",
                  "type",
                  arrayType,
                  "elemType",
                  ctx.type(elemType),
                  "value",
                  value,
                  "dim",
                  dimExprCode.value());
          codeBuilder.append(code);
        }
      } else {
        // create array with init value
        int len = elements.length;
        StringBuilder argsBuilder = new StringBuilder();
        if (len > 0) {
          for (int i = 0; i < len; i++) {
            Expression argExpr = elements[i];
            ExprCode argExprCode = argExpr.genCode(ctx);
            if (isNotBlank(argExprCode.code())) {
              codeBuilder.append(argExprCode.code()).append("\n");
            }
            if (i != 0) {
              argsBuilder.append(", ");
            }
            argsBuilder.append(argExprCode.value());
          }
        }

        String code =
            format(
                "${type} ${value} = new ${type} {${args}};",
                "type",
                arrayType,
                "value",
                value,
                "args",
                argsBuilder.toString());
        codeBuilder.append(code);
      }

      return new ExprCode(codeBuilder.toString(), null, Code.variable(rawType, value));
    }
  }

  class If implements Expression {
    private Expression predicate;
    private Expression trueExpr;
    private Expression falseExpr;
    private TypeToken<?> type;
    private boolean nullable;

    public If(Expression predicate, Expression trueExpr) {
      this.predicate = predicate;
      this.trueExpr = trueExpr;
      this.nullable = false;
      this.type = PRIMITIVE_VOID_TYPE;
    }

    public If(Expression predicate, Expression trueExpr, Expression falseExpr, boolean nullable) {
      this(predicate, trueExpr, falseExpr);
      this.nullable = nullable;
    }

    /** if predicate eval to null, take predicate as false */
    public If(Expression predicate, Expression trueExpr, Expression falseExpr) {
      this.predicate = predicate;
      this.trueExpr = trueExpr;
      this.falseExpr = falseExpr;

      if (trueExpr.type() == falseExpr.type()) {
        if (trueExpr.type() != null && !PRIMITIVE_VOID_TYPE.equals(trueExpr.type())) {
          type = trueExpr.type();
        } else {
          type = PRIMITIVE_VOID_TYPE;
        }
      } else {
        if (trueExpr.type() != null
            && !PRIMITIVE_VOID_TYPE.equals(trueExpr.type())
            && falseExpr.type() != null
            && !PRIMITIVE_VOID_TYPE.equals(falseExpr.type())) {
          type = OBJECT_TYPE;
        } else {
          type = PRIMITIVE_VOID_TYPE;
        }
      }
      nullable = !PRIMITIVE_VOID_TYPE.equals(type);
    }

    @Override
    public TypeToken<?> type() {
      return type;
    }

    @Override
    public ExprCode doGenCode(CodegenContext ctx) {
      ExprCode condEval = predicate.doGenCode(ctx);
      ExprCode trueEval = trueExpr.doGenCode(ctx);
      StringBuilder codeBuilder = new StringBuilder();
      if (isNotBlank(condEval.code())) {
        codeBuilder.append(condEval.code()).append('\n');
      }
      String cond;
      if (!"false".equals(condEval.isNull().code())) {
        // indicate condEval.isNull() is a variable. "false" is a java keyword, thus is not a
        // variable
        cond =
            format(
                "!${condEvalIsNull} && ${condEvalValue}",
                "condEvalIsNull",
                condEval.isNull(),
                "condEvalValue",
                condEval.value());
      } else {
        cond = format("${condEvalValue}", "condEvalValue", condEval.value());
      }

      if (!PRIMITIVE_VOID_TYPE.equals(type)) {
        ExprCode falseEval = falseExpr.doGenCode(ctx);
        Preconditions.checkArgument(trueEval.isNull() != null || falseEval.isNull() != null);
        Preconditions.checkNotNull(trueEval.value());
        Preconditions.checkNotNull(falseEval.value());
        Class<?> rawType = type.getRawType();
        String[] freshNames = ctx.freshNames(rawType, "isNull");
        String value = freshNames[0];
        String isNull = freshNames[1];
        codeBuilder.append(String.format("%s %s;\n", ctx.type(type), value));
        String ifCode;
        if (nullable) {
          codeBuilder.append(String.format("boolean %s = false;\n", isNull));
          String trueEvalIsNull;
          if (trueEval.isNull() == null) {
            trueEvalIsNull = "false";
          } else {
            trueEvalIsNull = trueEval.isNull().code();
          }
          String falseEvalIsNull;
          if (falseEval.isNull() == null) {
            falseEvalIsNull = "false";
          } else {
            falseEvalIsNull = falseEval.isNull().code();
          }
          ifCode =
              format(
                  ""
                      + "if (${cond}) {\n"
                      + "    ${trueEvalCode}\n"
                      + "    ${isNull} = ${trueEvalIsNull};\n"
                      + "    ${value} = ${trueEvalValue};\n"
                      + "} else {\n"
                      + "    ${falseEvalCode}\n"
                      + "    ${isNull} = ${falseEvalIsNull};\n"
                      + "    ${value} = ${falseEvalValue};\n"
                      + "}",
                  "isNull",
                  isNull,
                  "value",
                  value,
                  "cond",
                  cond,
                  "trueEvalCode",
                  alignIndent(trueEval.code()),
                  "trueEvalIsNull",
                  trueEvalIsNull,
                  "trueEvalValue",
                  trueEval.value(),
                  "falseEvalCode",
                  alignIndent(falseEval.code()),
                  "falseEvalIsNull",
                  falseEvalIsNull,
                  "falseEvalValue",
                  falseEval.value());
        } else {
          ifCode =
              format(
                  ""
                      + "if (${cond}) {\n"
                      + "    ${trueEvalCode}\n"
                      + "    ${value} = ${trueEvalValue};\n"
                      + "} else {\n"
                      + "    ${falseEvalCode}\n"
                      + "    ${value} = ${falseEvalValue};\n"
                      + "}",
                  "cond",
                  cond,
                  "value",
                  value,
                  "trueEvalCode",
                  alignIndent(trueEval.code()),
                  "trueEvalValue",
                  trueEval.value(),
                  "falseEvalCode",
                  alignIndent(falseEval.code()),
                  "falseEvalValue",
                  falseEval.value());
        }
        codeBuilder.append(stripBlankLines(ifCode));
        return new ExprCode(
            codeBuilder.toString(), Code.isNullVariable(isNull), Code.variable(rawType, value));
      } else {
        String ifCode;
        if (falseExpr != null) {
          ExprCode falseEval = falseExpr.doGenCode(ctx);
          ifCode =
              format(
                  "if (${cond}) {\n"
                      + "    ${trueEvalCode}\n"
                      + "} else {\n"
                      + "    ${falseEvalCode}\n"
                      + "}",
                  "cond",
                  cond,
                  "trueEvalCode",
                  alignIndent(trueEval.code()),
                  "falseEvalCode",
                  alignIndent(falseEval.code()));
        } else {
          ifCode =
              format(
                  "if (${cond}) {\n" + "    ${trueEvalCode}\n" + "}",
                  "cond",
                  cond,
                  "trueEvalCode",
                  alignIndent(trueEval.code()));
        }
        codeBuilder.append(ifCode);
        return new ExprCode(codeBuilder.toString());
      }
    }

    @Override
    public boolean nullable() {
      return nullable;
    }

    @Override
    public String toString() {
      if (falseExpr != null) {
        return String.format("if (%s) %s else %s", predicate, trueExpr, falseExpr);
      } else {
        return String.format("if (%s) %s", predicate, trueExpr);
      }
    }
  }

  class IsNull implements Expression {
    private Expression expr;

    public IsNull(Expression expr) {
      this.expr = expr;
    }

    @Override
    public TypeToken<?> type() {
      return PRIMITIVE_BOOLEAN_TYPE;
    }

    @Override
    public ExprCode doGenCode(CodegenContext ctx) {
      ExprCode targetExprCode = expr.genCode(ctx);
      Preconditions.checkNotNull(targetExprCode.isNull());
      return new ExprCode(targetExprCode.code(), FalseLiteral, targetExprCode.isNull());
    }

    @Override
    public boolean nullable() {
      return false;
    }

    @Override
    public String toString() {
      return String.format("IsNull(%s)", expr);
    }
  }

  class Not implements Expression {
    private final Expression target;

    public Not(Expression target) {
      this.target = target;
      Preconditions.checkArgument(
          target.type() == PRIMITIVE_BOOLEAN_TYPE || target.type() == BOOLEAN_TYPE);
    }

    @Override
    public TypeToken<?> type() {
      return target.type();
    }

    @Override
    public ExprCode doGenCode(CodegenContext ctx) {
      ExprCode targetExprCode = target.genCode(ctx);
      // whether need to check null for BOOLEAN_TYPE. The question is what to do when target.value
      // is null.
      String value = String.format("(!%s)", targetExprCode.value());
      return new ExprCode(targetExprCode.code(), FalseLiteral, Code.variable(boolean.class, value));
    }

    @Override
    public boolean nullable() {
      return false;
    }

    @Override
    public String toString() {
      return String.format("!(%s)", target);
    }
  }

  class Comparator extends ValueExpression {
    private final String operator;
    private final Expression left;
    private final Expression right;
    private boolean inline;

    public Comparator(String operator, Expression left, Expression right, boolean inline) {
      this.operator = operator;
      this.left = left;
      this.right = right;
      this.inline = inline;
    }

    @Override
    public TypeToken<?> type() {
      return PRIMITIVE_BOOLEAN_TYPE;
    }

    @Override
    public ExprCode doGenCode(CodegenContext ctx) {
      StringBuilder codeBuilder = new StringBuilder();
      ExprCode leftExprCode = left.genCode(ctx);
      ExprCode rightExprCode = right.genCode(ctx);
      Stream.of(leftExprCode, rightExprCode)
          .forEach(
              exprCode -> {
                if (isNotBlank(exprCode.code())) {
                  codeBuilder.append(exprCode.code()).append('\n');
                }
              });
      if (!inline) {
        String value = ctx.freshName(valuePrefix);
        String code =
            format(
                "boolean ${value} = ${leftValue} ${operator} ${rightValue};",
                "value",
                value,
                "leftValue",
                leftExprCode.value(),
                "operator",
                operator,
                "rightValue",
                rightExprCode.value());
        codeBuilder.append(code);
        return new ExprCode(
            codeBuilder.toString(), FalseLiteral, Code.variable(boolean.class, value));
      } else {
        String value =
            format(
                "(${leftValue} ${operator} ${rightValue})",
                "leftValue",
                leftExprCode.value(),
                "operator",
                operator,
                "rightValue",
                rightExprCode.value());
        return new ExprCode(
            (stripIfHasLastNewline(codeBuilder)).toString(),
            FalseLiteral,
            Code.variable(boolean.class, value));
      }
    }

    public String toString() {
      return String.format("%s %s %s", left, operator, right);
    }
  }

  class Arithmetic extends ValueExpression {
    private final boolean inline;
    private final String operator;
    private final Expression[] operands;
    private final TypeToken<?> type;

    public Arithmetic(String operator, Expression... operands) {
      this(false, operator, operands);
    }

    public Arithmetic(boolean inline, String operator, Expression... operands) {
      this.inline = inline;
      this.operator = operator;
      this.operands = operands;
      Preconditions.checkArgument(operands.length >= 2);
      TypeToken<?> t = null;
      for (Expression operand : operands) {
        if (t != null) {
          Preconditions.checkArgument(t == operand.type());
        } else {
          t = operand.type();
        }
      }
      type = t;
    }

    @Override
    public TypeToken<?> type() {
      return type;
    }

    @Override
    public ExprCode doGenCode(CodegenContext ctx) {
      StringBuilder codeBuilder = new StringBuilder();
      StringBuilder arith = new StringBuilder();
      for (int i = 0; i < operands.length; i++) {
        Expression operand = operands[i];
        ExprCode code = operand.genCode(ctx);
        if (isNotBlank(code.code())) {
          appendNewlineIfNeeded(codeBuilder);
          codeBuilder.append(code.code());
        }
        if (i != operands.length - 1) {
          arith.append(code.value()).append(' ').append(operator).append(' ');
        } else {
          arith.append(code.value());
        }
      }

      if (inline) {
        String value = String.format("(%s)", arith);
        String code = isBlank(codeBuilder) ? null : codeBuilder.toString();
        return new ExprCode(code, FalseLiteral, Code.variable(type.getRawType(), value));
      } else {
        appendNewlineIfNeeded(codeBuilder);
        String value = ctx.freshName(valuePrefix);
        String valueExpr =
            format(
                "${type} ${value} = ${arith};",
                "type",
                ctx.type(type),
                "value",
                value,
                "arith",
                arith);
        codeBuilder.append(valueExpr);
        return new ExprCode(
            codeBuilder.toString(), FalseLiteral, Code.variable(type.getRawType(), value));
      }
    }
  }

  class Add extends Arithmetic {
    public Add(Expression... operands) {
      super("+", operands);
    }

    public Add(boolean inline, Expression... operands) {
      super(inline, "+", operands);
    }
  }

  class ForEach implements Expression {
    private final Expression inputObject;
    private final BiFunction<Expression, Expression, Expression> action;
    private final TypeToken<?> elementType;

    /**
     * inputObject.type() must be multi-dimension array or Collection, not allowed to be primitive
     * array
     */
    public ForEach(Expression inputObject, BiFunction<Expression, Expression, Expression> action) {
      this.inputObject = inputObject;
      this.action = action;
      if (inputObject.type().isArray()) {
        elementType = inputObject.type().getComponentType();
      } else {
        elementType = getElementType(inputObject.type());
      }
    }

    @Override
    public TypeToken<?> type() {
      return PRIMITIVE_VOID_TYPE;
    }

    @Override
    public ExprCode doGenCode(CodegenContext ctx) {
      StringBuilder codeBuilder = new StringBuilder();
      ExprCode targetExprCode = inputObject.genCode(ctx);
      if (isNotBlank(targetExprCode.code())) {
        codeBuilder.append(targetExprCode.code()).append("\n");
      }
      String i = ctx.freshName("i");
      String elemValue = ctx.freshName("elemValue");
      Expression elementExpr =
          action.apply(new Literal(i), new Reference(elemValue, elementType, true));
      ExprCode elementExprCode = elementExpr.genCode(ctx);

      if (inputObject.type().isArray()) {
        String code =
            format(
                ""
                    + "int ${len} = ${arr}.length;\n"
                    + "int ${i} = 0;\n"
                    + "while (${i} < ${len}) {\n"
                    + "    ${elemType} ${elemValue} = ${arr}[${i}];\n"
                    + "    ${elementExprCode}\n"
                    + "    ${i}++;\n"
                    + "}",
                "arr",
                targetExprCode.value(),
                "len",
                ctx.freshName("len"),
                "i",
                i,
                "elemType",
                ctx.type(elementType),
                "elemValue",
                elemValue,
                "i",
                i,
                "elementExprCode",
                alignIndent(elementExprCode.code()));
        codeBuilder.append(code);
        return new ExprCode(codeBuilder.toString(), null, null);
      } else {
        Preconditions.checkArgument(
            ITERABLE_TYPE.isSupertypeOf(inputObject.type()),
            "Unsupported type " + inputObject.type());
        String code =
            format(
                ""
                    + "Iterator ${iter} = ${input}.iterator();\n"
                    + "int ${i} = 0;\n"
                    + "while (${iter}.hasNext()) {\n"
                    + "    ${elemType} ${elemValue} = (${elemType})${iter}.next();\n"
                    + "    ${elementExprCode}\n"
                    + "    ${i}++;\n"
                    + "}",
                "iter",
                ctx.freshName("iter"),
                "i",
                i,
                "input",
                targetExprCode.value().code(),
                "elemType",
                ctx.type(elementType),
                "elemValue",
                elemValue,
                "elementExprCode",
                alignIndent(elementExprCode.code()));
        codeBuilder.append(code);
        return new ExprCode(codeBuilder.toString(), null, null);
      }
    }

    @Override
    public String toString() {
      return String.format("ForEach(%s, %s)", inputObject, action);
    }
  }

  class ForLoop implements Expression {
    private final Expression start;
    private final Expression end;
    private final Expression step;
    private final Function<Expression, Expression> action;

    public ForLoop(
        Expression start,
        Expression end,
        Expression step,
        Function<Expression, Expression> action) {
      this.start = start;
      this.end = end;
      this.step = step;
      this.action = action;
    }

    @Override
    public TypeToken<?> type() {
      return PRIMITIVE_VOID_TYPE;
    }

    @Override
    public ExprCode doGenCode(CodegenContext ctx) {
      Class<?> maxType = maxType(start.type().getRawType(), end.type().getRawType());
      Preconditions.checkArgument(maxType.isPrimitive());
      StringBuilder codeBuilder = new StringBuilder();
      String i = ctx.freshName("i");
      Reference iRef = new Reference(i, TypeToken.of(maxType));
      Expression loopAction = action.apply(iRef);
      ExprCode startExprCode = start.genCode(ctx);
      ExprCode endExprCode = end.genCode(ctx);
      ExprCode stepExprCode = step.genCode(ctx);
      ExprCode actionExprCode = loopAction.genCode(ctx);
      Stream.of(startExprCode, endExprCode, stepExprCode)
          .forEach(
              exprCode -> {
                if (isNotBlank(exprCode.code())) {
                  codeBuilder.append(exprCode.code()).append('\n');
                }
              });

      String forCode =
          format(
              ""
                  + "for (${type} ${i} = ${start}; ${i} < ${end}; ${i}+=${step}) {\n"
                  + "   ${actionCode}\n"
                  + "}",
              "type",
              maxType.toString(),
              "i",
              i,
              "start",
              startExprCode.value(),
              "end",
              endExprCode.value(),
              "step",
              stepExprCode.value(),
              "actionCode",
              actionExprCode.code());
      codeBuilder.append(forCode);
      return new ExprCode(codeBuilder.toString(), null, null);
    }
  }

  class Return implements Expression {
    private final Expression expression;

    public Return(Expression expression) {
      this.expression = expression;
    }

    @Override
    public TypeToken<?> type() {
      return expression.type();
    }

    @Override
    public ExprCode doGenCode(CodegenContext ctx) {
      StringBuilder codeBuilder = new StringBuilder();
      ExprCode targetExprCode = expression.genCode(ctx);
      if (isNotBlank(targetExprCode.code())) {
        codeBuilder.append(targetExprCode.code()).append('\n');
      }
      codeBuilder.append("return ").append(targetExprCode.value()).append(';');
      return new ExprCode(codeBuilder.toString(), null, null);
    }

    @Override
    public String toString() {
      return String.format("return %s", expression);
    }
  }
}
