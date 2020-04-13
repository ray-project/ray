package io.ray.streaming.python;

import io.ray.streaming.api.Language;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.operator.OperatorType;
import io.ray.streaming.operator.StreamOperator;
import java.util.List;

/**
 * Represents a {@link StreamOperator} that wraps python {@link PythonFunction}.
 */
@SuppressWarnings("unchecked")
public class PythonOperator extends StreamOperator {

  public PythonOperator(PythonFunction function) {
    super(function);
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
}
