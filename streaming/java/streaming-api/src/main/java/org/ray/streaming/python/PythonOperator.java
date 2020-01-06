package org.ray.streaming.python;

import org.ray.streaming.operator.OperatorType;
import org.ray.streaming.operator.StreamOperator;
import org.ray.streaming.python.descriptor.DescriptorFunction;

/**
 * PythonOperator is a {@link StreamOperator} that wraps python {@link DescriptorFunction}.
 */
@SuppressWarnings("unchecked")
public class PythonOperator extends StreamOperator {

  public PythonOperator(DescriptorFunction function) {
    super(function);
  }

  @Override
  public OperatorType getOpType() {
    throw new RuntimeException("DescriptorOperator methods shouldn't be called in java");
  }

}
