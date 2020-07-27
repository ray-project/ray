package io.ray.streaming.operator.impl;

import io.ray.streaming.api.function.impl.JoinFunction;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.ChainStrategy;
import io.ray.streaming.operator.OperatorType;
import io.ray.streaming.operator.StreamOperator;
import io.ray.streaming.operator.TwoInputOperator;

/**
 * Join operator
 *
 * @param <L> Type of the data in the left stream.
 * @param <R> Type of the data in the right stream.
 * @param <K> Type of the data in the join key.
 * @param <O> Type of the data in the joined stream.
 */
public class JoinOperator<L, R, K, O> extends StreamOperator<JoinFunction<L, R, O>> implements
    TwoInputOperator<L, R> {
  public JoinOperator() {

  }

  public JoinOperator(JoinFunction<L, R, O> function) {
    super(function);
    setChainStrategy(ChainStrategy.HEAD);
  }

  @Override
  public void processElement(Record<L> record1, Record<R> record2) {

  }

  @Override
  public OperatorType getOpType() {
    return OperatorType.TWO_INPUT;
  }

}
