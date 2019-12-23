package org.ray.streaming.runtime.core.processor;

import org.ray.streaming.operator.OneInputOperator;
import org.ray.streaming.operator.OperatorType;
import org.ray.streaming.operator.StreamOperator;
import org.ray.streaming.operator.TwoInputOperator;
import org.ray.streaming.operator.impl.SourceOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProcessBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessBuilder.class);

  public static StreamProcessor buildProcessor(StreamOperator streamOperator) {
    OperatorType type = streamOperator.getOpType();
    LOGGER.info("Building StreamProcessor, operator type = {}, operator = {}.", type,
        streamOperator.getClass().getSimpleName().toString());
    switch (type) {
      case SOURCE:
        return new SourceProcessor<>((SourceOperator) streamOperator);
      case ONE_INPUT:
        return new OneInputProcessor<>((OneInputOperator) streamOperator);
      case TWO_INPUT:
        return new TwoInputProcessor((TwoInputOperator) streamOperator);
      default:
        throw new RuntimeException("current operator type is not support");
    }
  }
}
