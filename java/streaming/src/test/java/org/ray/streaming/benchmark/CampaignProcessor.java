package org.ray.streaming.benchmark;

import org.ray.streaming.api.collector.Collector;
import org.ray.streaming.api.function.impl.FlatMapFunction;
import org.ray.streaming.benchmark.common.advertising.CampaignProcessorCommon;
import org.ray.streaming.util.Record3;

public class CampaignProcessor implements FlatMapFunction<Record3, String> {

  private CampaignProcessorCommon campaignProcessorCommon;
  private boolean isFirst = true;

  @Override
  public void flatMap(Record3 record3, Collector<String> collector) {
    if (isFirst) {
      // NOTE(zhenxuanpan)
      this.campaignProcessorCommon = new CampaignProcessorCommon("");
      this.campaignProcessorCommon.prepare();
      this.isFirst = false;
    }

    String campaign_id = record3.getF0();
    String event_time = record3.getF2();
    this.campaignProcessorCommon.execute(campaign_id, event_time);
    collector.collect(campaign_id);
  }
}
