package io.ray.serialization.codegen;

import static io.ray.serialization.codegen.Expression.Arithmetic;
import static io.ray.serialization.codegen.Expression.Comparator;
import static io.ray.serialization.codegen.Expression.IsNull;
import static io.ray.serialization.codegen.Expression.Literal;
import static io.ray.serialization.codegen.Expression.NewArray;
import static io.ray.serialization.codegen.Expression.Not;
import static io.ray.serialization.codegen.Expression.StaticInvoke;
import static io.ray.serialization.util.StringUtils.format;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

@SuppressWarnings("UnstableApiUsage")
public class ExpressionUtils {

  public static Expression newObjectArray(Expression... expressions) {
    return new NewArray(TypeToken.of(Object[].class), expressions);
  }

  public static Expression valueOf(TypeToken<?> type, Expression value) {
    return new StaticInvoke(type.getRawType(), "valueOf", type, false, value);
  }

  public static IsNull isNull(Expression target) {
    return new IsNull(target);
  }

  public static Expression notNull(Expression target) {
    return new Not(new IsNull(target));
  }

  public static Expression eqNull(Expression target) {
    Preconditions.checkArgument(!target.type().isPrimitive());
    return eq(target, new Literal("null"));
  }

  public static Not not(Expression target) {
    return new Not(target);
  }

  public static Literal nullValue(TypeToken<?> type) {
    return new Literal(null, type);
  }

  public static Literal literalStr(String value) {
    value = String.format("\"%s\"", value);
    return new Literal(value);
  }

  public static Comparator eq(Expression left, Expression right) {
    return new Comparator("==", left, right, true);
  }

  public static Comparator eq(Expression left, Expression right, String valuePrefix) {
    Comparator comparator = new Comparator("==", left, right, false);
    comparator.valuePrefix = valuePrefix;
    return comparator;
  }

  public static Arithmetic add(Expression left, Expression right) {
    return new Arithmetic(true, "+", left, right);
  }

  public static Arithmetic add(Expression left, Expression right, String valuePrefix) {
    Arithmetic arithmetic = new Arithmetic(true, "+", left, right);
    arithmetic.valuePrefix = valuePrefix;
    return arithmetic;
  }

  public static Arithmetic subtract(Expression left, Expression right) {
    return new Arithmetic(true, "-", left, right);
  }

  public static Arithmetic subtract(Expression left, Expression right, String valuePrefix) {
    Arithmetic arithmetic = new Arithmetic(true, "-", left, right);
    arithmetic.valuePrefix = valuePrefix;
    return arithmetic;
  }

  static String callFunc(
      String type,
      String resultVal,
      String target,
      String functionName,
      String args,
      boolean needTryCatch) {
    if (needTryCatch) {
      return format(
          "${type} ${value};\n"
              + "try {\n"
              + "   ${value} = ${target}.${functionName}(${args});\n"
              + "} catch (Exception e) {\n"
              + "   throw new RuntimeException(e);\n"
              + "}",
          "type",
          type,
          "value",
          resultVal,
          "target",
          target,
          "functionName",
          functionName,
          "args",
          args);
    } else {
      return format(
          "${type} ${value} = ${target}.${functionName}(${args});",
          "type",
          type,
          "value",
          resultVal,
          "target",
          target,
          "functionName",
          functionName,
          "args",
          args);
    }
  }

  static String callFunc(String target, String functionName, String args, boolean needTryCatch) {
    if (needTryCatch) {
      return format(
          "try {\n"
              + "   ${target}.${functionName}(${args});\n"
              + "} catch (Exception e) {\n"
              + "   throw new RuntimeException(e);\n"
              + "}",
          "target",
          target,
          "functionName",
          functionName,
          "args",
          args);
    } else {
      return format(
          "${target}.${functionName}(${args});",
          "target",
          target,
          "functionName",
          functionName,
          "args",
          args);
    }
  }
}
