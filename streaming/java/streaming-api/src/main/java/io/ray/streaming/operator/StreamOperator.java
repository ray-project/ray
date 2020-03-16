package io.ray.streaming.operator;

import io.ray.streaming.api.Language;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.function.Function;
import io.ray.streaming.message.KeyRecord;
import io.ray.streaming.message.Record;
import java.util.List;
<<<<<<< HEAD:streaming/java/streaming-api/src/main/java/io/ray/streaming/operator/StreamOperator.java
=======
import org.ray.streaming.api.Language;
import org.ray.streaming.api.collector.Collector;
import org.ray.streaming.api.context.StreamRuntimeContext;
import org.ray.streaming.api.function.Function;
import org.ray.streaming.message.KeyRecord;
import org.ray.streaming.message.Record;
>>>>>>> add state module:streaming/java/streaming-api/src/main/java/org/ray/streaming/operator/StreamOperator.java

public abstract class StreamOperator<F extends Function> implements Operator {
  protected String name;
  protected F function;
  protected List<Collector> collectorList;
  protected StreamRuntimeContext runtimeContext;

  public StreamOperator(F function) {
    this.name = getClass().getSimpleName();
    this.function = function;
  }

  @Override
  public void open(List<Collector> collectorList, StreamRuntimeContext runtimeContext) {
    this.collectorList = collectorList;
    this.runtimeContext = runtimeContext;
  }

  @Override
  public void finish() {

  }

  @Override
  public void close() {

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

  public String getName() {
    return name;
  }
}
