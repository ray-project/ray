package org.ray.streaming.benchmark;

import org.ray.streaming.api.collector.Collector;
import org.ray.streaming.api.function.impl.FlatMapFunction;
import org.ray.streaming.util.Record7;

public class FilterUsingFlatMap implements FlatMapFunction<Record7, Record7> {

  @Override
  public void flatMap(Record7 record7, Collector<Record7> collector) {
    if (record7.getF4().equals("view")) {
      collector.collect(record7);
    }
  }

}
