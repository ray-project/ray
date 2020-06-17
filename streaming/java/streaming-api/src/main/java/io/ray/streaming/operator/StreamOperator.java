package io.ray.streaming.operator;

import io.ray.streaming.api.Language;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.function.Function;
import io.ray.streaming.api.function.RichFunction;
import io.ray.streaming.api.function.internal.Functions;
import io.ray.streaming.message.KeyRecord;
import io.ray.streaming.message.Record;
import java.util.List;

public abstract class StreamOperator<F extends Function> implements Operator {
  protected final String name;
  protected F function;
  protected RichFunction richFunction;
  protected List<Collector> collectorList;
  protected RuntimeContext runtimeContext;
  private ChainStrategy chainStrategy = ChainStrategy.ALWAYS;

  protected StreamOperator() {
    this.name = getClass().getSimpleName();
  }

  protected StreamOperator(F function) {
    this();
    setFunction(function);
  }

  public void setFunction(F function) {
    this.function = function;
    this.richFunction = Functions.wrap(function);
  }

  @Override
  public void open(List<Collector> collectorList, RuntimeContext runtimeContext) {
    this.collectorList = collectorList;
    this.runtimeContext = runtimeContext;
    richFunction.open(runtimeContext);
  }

  @Override
  public void finish() {

  }

  @Override
  public void close() {
    richFunction.close();
  }

  @Override
  public Function getFunction() {
    return function;
  }

  @Override
  public Language getLanguage() {
    return Language.JAVA;
  }

  protected void collect(Record record) {
    for (Collector collector : this.collectorList) {
      collector.collect(record);
    }
  }

  protected void collect(KeyRecord keyRecord) {
    for (Collector collector : this.collectorList) {
      collector.collect(keyRecord);
    }
  }

  @Override
  public String getName() {
    return name;
  }

  public void setChainStrategy(ChainStrategy chainStrategy) {
    this.chainStrategy = chainStrategy;
  }

  @Override
  public ChainStrategy getChainStrategy() {
    return chainStrategy;
  }
}
