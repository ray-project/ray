package com.ray.streaming.operator;

import com.ray.streaming.api.collector.Collector;
import com.ray.streaming.core.runtime.context.RuntimeContext;
import java.io.Serializable;
import java.util.List;

public interface Operator extends Serializable {

  void open(List<Collector> collectors, RuntimeContext runtimeContext);

  void finish();

  void close();

  OperatorType getOpType();
}
