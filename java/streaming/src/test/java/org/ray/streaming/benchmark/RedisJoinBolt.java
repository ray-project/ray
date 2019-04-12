package org.ray.streaming.benchmark;


import org.ray.streaming.api.collector.Collector;
import org.ray.streaming.api.function.impl.FlatMapFunction;
import org.ray.streaming.benchmark.common.advertising.RedisAdCampaignCache;
import org.ray.streaming.util.Record2;
import org.ray.streaming.util.Record3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisJoinBolt implements FlatMapFunction<Record2, Record3> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RedisJoinBolt.class);

  RedisAdCampaignCache redisAdCampaignCache;
  private int eventCount = 0;
  private boolean isFirst = true;

  @Override
  public void flatMap(Record2 record2, Collector<Record3> collector) {
    if (isFirst) {
      // NOTE(zhenxuanpan)
      this.redisAdCampaignCache = new RedisAdCampaignCache("");
      this.redisAdCampaignCache.prepare();
      this.isFirst = false;
    }

    String ad_id = record2.getF0();
    String campaign_id = this.redisAdCampaignCache.execute(ad_id);
    if (campaign_id == null) {
      return;
    }

    eventCount++;
    if (eventCount % 100 == 0 || eventCount == 0) {
      LOGGER.info("RedisJoinBolt eventCount: {}", eventCount);
    }
    Record3 record3 = new Record3(
        campaign_id,
        record2.getF0(),
        record2.getF1());
    collector.collect(record3);
  }
}


