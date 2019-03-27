package com.ray.streaming.operator;

import com.ray.streaming.api.collector.Collector;
import com.ray.streaming.api.function.Function;
import com.ray.streaming.core.runtime.context.RuntimeContext;
import com.ray.streaming.message.KeyRecord;
import com.ray.streaming.message.Record;
import java.util.List;

public abstract class StreamOperator<F extends Function> implements Operator {

  protected F function;
  protected List<Collector> collectorList;
  protected RuntimeContext runtimeContext;


  public StreamOperator(F function) {
    this.function = function;
  }

  public void open(List<Collector> collectorList, RuntimeContext runtimeContext) {
    this.collectorList = collectorList;
    this.runtimeContext = runtimeContext;
  }

  public void finish() {

  }

  public void close() {

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

}
