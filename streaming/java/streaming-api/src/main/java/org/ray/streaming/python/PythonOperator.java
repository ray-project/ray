package org.ray.streaming.python;

import java.util.List;
import org.ray.streaming.api.Language;
import org.ray.streaming.api.context.RuntimeContext;
import org.ray.streaming.operator.OperatorType;
import org.ray.streaming.operator.StreamOperator;

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
