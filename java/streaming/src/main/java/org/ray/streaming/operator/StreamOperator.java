package org.ray.streaming.operator;

import java.util.List;
import org.ray.streaming.api.collector.Collector;
import org.ray.streaming.api.function.Function;
import org.ray.streaming.core.runtime.context.RuntimeContext;
import org.ray.streaming.message.KeyRecord;
import org.ray.streaming.message.Record;

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
