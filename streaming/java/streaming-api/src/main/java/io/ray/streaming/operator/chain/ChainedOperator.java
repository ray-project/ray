package io.ray.streaming.operator.chain;

import com.google.common.base.Preconditions;
import io.ray.streaming.api.Language;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.function.Function;
import io.ray.streaming.api.function.impl.SourceFunction.SourceContext;
import io.ray.streaming.message.Record;
import io.ray.streaming.operator.OneInputOperator;
import io.ray.streaming.operator.Operator;
import io.ray.streaming.operator.OperatorType;
import io.ray.streaming.operator.SourceOperator;
import io.ray.streaming.operator.StreamOperator;
import io.ray.streaming.operator.TwoInputOperator;
import java.io.Serializable;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Abstract base class for chained operators. */
public abstract class ChainedOperator extends StreamOperator<Function> {

  protected final List<StreamOperator> operators;
  protected final Operator headOperator;
  protected final Operator tailOperator;
  private final List<Map<String, String>> configs;

  public ChainedOperator(List<StreamOperator> operators, List<Map<String, String>> configs) {
    Preconditions.checkArgument(
        operators.size() >= 2, "Need at lease two operators to be chained together");
    operators.stream()
        .skip(1)
        .forEach(operator -> Preconditions.checkArgument(operator instanceof OneInputOperator));
    this.operators = operators;
    this.configs = configs;
    this.headOperator = operators.get(0);
    this.tailOperator = operators.get(operators.size() - 1);
  }

  @Override
  public void open(List<Collector> collectorList, RuntimeContext runtimeContext) {
    // Dont' call super.open() as we `open` every operator separately.
    List<ForwardCollector> succeedingCollectors =
        operators.stream()
            .skip(1)
            .map(operator -> new ForwardCollector((OneInputOperator) operator))
            .collect(Collectors.toList());
    for (int i = 0; i < operators.size() - 1; i++) {
      StreamOperator operator = operators.get(i);
      List<ForwardCollector> forwardCollectors =
          Collections.singletonList(succeedingCollectors.get(i));
      operator.open(forwardCollectors, createRuntimeContext(runtimeContext, i));
    }
    // tail operator send data to downstream using provided collectors.
    tailOperator.open(collectorList, createRuntimeContext(runtimeContext, operators.size() - 1));
  }

  @Override
  public OperatorType getOpType() {
    return headOperator.getOpType();
  }

  @Override
  public Language getLanguage() {
    return headOperator.getLanguage();
  }

  @Override
  public String getName() {
    return operators.stream().map(Operator::getName).collect(Collectors.joining(" -> ", "[", "]"));
  }

  public List<StreamOperator> getOperators() {
    return operators;
  }

  public Operator getHeadOperator() {
    return headOperator;
  }

  public Operator getTailOperator() {
    return tailOperator;
  }

  @Override
  public Serializable saveCheckpoint() {
    Serializable[] checkpoints = new Serializable[operators.size()];
    for (int i = 0; i < operators.size(); ++i) {
      checkpoints[i] = operators.get(i).saveCheckpoint();
    }
    return checkpoints;
  }

  @Override
  public void loadCheckpoint(Serializable checkpointObject) {
    Serializable[] checkpoints = (Serializable[]) checkpointObject;
    for (int i = 0; i < operators.size(); ++i) {
      operators.get(i).loadCheckpoint(checkpoints[i]);
    }
  }

  private RuntimeContext createRuntimeContext(RuntimeContext runtimeContext, int index) {
    return (RuntimeContext)
        Proxy.newProxyInstance(
            runtimeContext.getClass().getClassLoader(),
            new Class[] {RuntimeContext.class},
            (proxy, method, methodArgs) -> {
              if (method.getName().equals("getConfig")) {
                return configs.get(index);
              } else {
                return method.invoke(runtimeContext, methodArgs);
              }
            });
  }

  public static ChainedOperator newChainedOperator(
      List<StreamOperator> operators, List<Map<String, String>> configs) {
    switch (operators.get(0).getOpType()) {
      case SOURCE:
        return new ChainedSourceOperator(operators, configs);
      case ONE_INPUT:
        return new ChainedOneInputOperator(operators, configs);
      case TWO_INPUT:
        return new ChainedTwoInputOperator(operators, configs);
      default:
        throw new IllegalArgumentException(
            "Unsupported operator type " + operators.get(0).getOpType());
    }
  }

  static class ChainedSourceOperator<T> extends ChainedOperator implements SourceOperator<T> {

    private final SourceOperator<T> sourceOperator;

    @SuppressWarnings("unchecked")
    ChainedSourceOperator(List<StreamOperator> operators, List<Map<String, String>> configs) {
      super(operators, configs);
      sourceOperator = (SourceOperator<T>) headOperator;
    }

    @Override
    public void fetch() {
      sourceOperator.fetch();
    }

    @Override
    public SourceContext<T> getSourceContext() {
      return sourceOperator.getSourceContext();
    }
  }

  static class ChainedOneInputOperator<T> extends ChainedOperator implements OneInputOperator<T> {

    private final OneInputOperator<T> inputOperator;

    @SuppressWarnings("unchecked")
    ChainedOneInputOperator(List<StreamOperator> operators, List<Map<String, String>> configs) {
      super(operators, configs);
      inputOperator = (OneInputOperator<T>) headOperator;
    }

    @Override
    public void processElement(Record<T> record) throws Exception {
      inputOperator.processElement(record);
    }
  }

  static class ChainedTwoInputOperator<L, R> extends ChainedOperator
      implements TwoInputOperator<L, R> {

    private final TwoInputOperator<L, R> inputOperator;

    @SuppressWarnings("unchecked")
    ChainedTwoInputOperator(List<StreamOperator> operators, List<Map<String, String>> configs) {
      super(operators, configs);
      inputOperator = (TwoInputOperator<L, R>) headOperator;
    }

    @Override
    public void processElement(Record<L> record1, Record<R> record2) {
      inputOperator.processElement(record1, record2);
    }
  }
}
