package io.ray.serialization.codegen;

import static io.ray.serialization.codegen.Code.LiteralValue.FalseLiteral;
import static io.ray.serialization.codegen.Code.LiteralValue.TrueLiteral;

import java.util.Objects;

public interface Code {

  /**
   * The code for a sequence of statements to evaluate the expression in a scope. If no code needs
   * to be evaluated, or expression is already evaluated in a scope ( see {@link
   * Expression#genCode(CodegenContext)}), thus `isNull` and `value` are already existed, the code
   * should be null.
   */
  class ExprCode {
    private String code;
    private ExprValue isNull;
    private ExprValue value;

    public ExprCode(String code) {
      this(code, null, null);
    }

    public ExprCode(ExprValue isNull, ExprValue value) {
      this(null, isNull, value);
    }

    /**
     * @param code The sequence of statements required to evaluate the expression. It should be
     *     null, if `isNull` and `value` are already existed, or no code needed to evaluate them
     *     (literals).
     * @param isNull A term that holds a boolean value representing whether the expression evaluated
     *     to null.
     * @param value A term for a (possibly primitive) value of the result of the evaluation. Not
     *     valid if `isNull` is set to `true`.
     */
    public ExprCode(String code, ExprValue isNull, ExprValue value) {
      this.code = code;
      this.isNull = isNull;
      this.value = value;
    }

    public String code() {
      return code;
    }

    public ExprValue isNull() {
      return isNull;
    }

    public ExprValue value() {
      return value;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("ExprCode(");
      if (code != null) {
        sb.append("code=\"").append('\n').append(code).append("\n\", ");
      }
      sb.append("isNull=").append(isNull);
      sb.append(", value=").append(value);
      sb.append(')');
      return sb.toString();
    }
  }

  /** Fragments of java code */
  abstract class JavaCode {

    abstract String code();

    @Override
    public String toString() {
      return code();
    }
  }

  /** A typed java fragment that must be a valid java expression. */
  abstract class ExprValue extends JavaCode {

    private Class<?> javaType;

    public ExprValue(Class<?> javaType) {
      this.javaType = javaType;
    }

    Class<?> javaType() {
      return javaType;
    }

    boolean isPrimitive() {
      return javaType.isPrimitive();
    }
  }

  /** A java expression fragment. */
  class SimpleExprValue extends ExprValue {

    private String expr;

    public SimpleExprValue(Class<?> javaType, String expr) {
      super(javaType);
      this.expr = expr;
    }

    @Override
    String code() {
      return String.format("(%s)", expr);
    }
  }

  /** A local variable java expression. */
  class VariableValue extends ExprValue {
    private String variableName;

    public VariableValue(Class<?> javaType, String variableName) {
      super(javaType);
      this.variableName = variableName;
    }

    @Override
    String code() {
      return variableName;
    }
  }

  /** A literal java expression. */
  class LiteralValue extends ExprValue {
    static LiteralValue TrueLiteral = new LiteralValue(boolean.class, "true");
    static LiteralValue FalseLiteral = new LiteralValue(boolean.class, "false");

    private String value;

    private LiteralValue(Class<?> javaType, String value) {
      super(javaType);
      this.value = value;
    }

    @Override
    String code() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      LiteralValue that = (LiteralValue) o;
      return this.javaType() == that.javaType() && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(value, javaType());
    }
  }

  // ########################## utils ##########################
  static LiteralValue literal(Object value) {
    return literal(value.getClass(), value.toString());
  }

  static LiteralValue literal(Class<?> type, String value) {
    if (type == Boolean.class || type == boolean.class) {
      if ("true".equals(value)) {
        return TrueLiteral;
      } else if ("false".equals(value)) {
        return FalseLiteral;
      } else {
        throw new IllegalArgumentException(value);
      }
    } else {
      return new LiteralValue(type, value);
    }
  }

  static ExprValue exprValue(Class<?> type, String code) {
    return new SimpleExprValue(type, code);
  }

  static ExprValue variable(Class<?> type, String name) {
    return new VariableValue(type, name);
  }

  static ExprValue isNullVariable(String name) {
    return new VariableValue(boolean.class, name);
  }
}
