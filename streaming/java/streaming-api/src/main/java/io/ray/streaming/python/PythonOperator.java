package io.ray.streaming.python;

import io.ray.streaming.api.Language;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.operator.OperatorType;
import io.ray.streaming.operator.StreamOperator;
import java.util.List;
import java.util.StringJoiner;

/**
 * Represents a {@link StreamOperator} that wraps python {@link PythonFunction}.
 */
@SuppressWarnings("unchecked")
public class PythonOperator extends StreamOperator {
  private final String moduleName;
  private final String className;

  public PythonOperator(String moduleName, String className) {
    super(null);
    this.moduleName = moduleName;
    this.className = className;
  }

  public PythonOperator(PythonFunction function) {
    super(function);
    this.moduleName = null;
    this.className = null;
  }

  @Override
  public void open(List list, RuntimeContext runtimeContext) {
    String msg = String.format("Methods of %s shouldn't be called.", getClass().getSimpleName());
    throw new UnsupportedOperationException(msg);
  }

  @Override
  public void finish() {
    String msg = String.format("Methods of %s shouldn't be called.", getClass().getSimpleName());
    throw new UnsupportedOperationException(msg);
  }

  @Override
  public void close() {
    String msg = String.format("Methods of %s shouldn't be called.", getClass().getSimpleName());
    throw new UnsupportedOperationException(msg);
  }

  @Override
  public OperatorType getOpType() {
    String msg = String.format("Methods of %s shouldn't be called.", getClass().getSimpleName());
    throw new UnsupportedOperationException(msg);
  }

  @Override
  public Language getLanguage() {
    return Language.PYTHON;
  }

  public String getModuleName() {
    return moduleName;
  }

  public String getClassName() {
    return className;
  }

  @Override
  public String toString() {
    StringJoiner stringJoiner = new StringJoiner(", ",
        PythonOperator.class.getSimpleName() + "[", "]");
    if (function != null) {
      stringJoiner.add("function='" + function + "'");
    } else {
      stringJoiner.add("moduleName='" + moduleName + "'")
          .add("className='" + className + "'");
    }
    return stringJoiner.toString();
  }
}
