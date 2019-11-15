package org.ray.streaming.operator;

import java.io.Serializable;
import java.util.List;
import org.ray.streaming.api.collector.Collector;
import org.ray.streaming.core.runtime.context.RuntimeContext;

public interface Operator extends Serializable {

  void open(List<Collector> collectors, RuntimeContext runtimeContext);

  void finish();

  void close();

  OperatorType getOpType();
}
