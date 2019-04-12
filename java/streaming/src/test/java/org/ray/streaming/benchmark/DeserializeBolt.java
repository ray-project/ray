package org.ray.streaming.benchmark;

import org.json.JSONObject;
import org.ray.streaming.api.collector.Collector;
import org.ray.streaming.api.function.impl.FlatMapFunction;
import org.ray.streaming.util.Record7;

public class DeserializeBolt implements FlatMapFunction<String, Record7> {

  @Override
  public void flatMap(String s, Collector<Record7> collector) {
    JSONObject obj = new JSONObject(s);
    Record7 record7 = new Record7(
        obj.getString("user_id"),
        obj.getString("page_id"),
        obj.getString("ad_id"),
        obj.getString("ad_type"),
        obj.getString("event_type"),
        obj.getString("event_time"),
        obj.getString("ip_address"));
    collector.collect(record7);
  }

}
