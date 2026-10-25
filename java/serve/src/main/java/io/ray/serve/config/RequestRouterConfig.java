package io.ray.serve.config;

import io.ray.serve.common.Constants;
import java.io.Serializable;

public class RequestRouterConfig implements Serializable {
  /** Frequency at which the controller will record request routing stats. */
  private Double requestRoutingStatsPeriodS = Constants.DEFAULT_REQUEST_ROUTING_STATS_PERIOD_S;

  /**
   * Timeout that the controller waits for a response from the replica's request routing stats
   * before retrying.
   */
  private Double requestRoutingStatsTimeoutS = Constants.DEFAULT_REQUEST_ROUTING_STATS_TIMEOUT_S;

  public Double getRequestRoutingStatsPeriodS() {
    return requestRoutingStatsPeriodS;
  }

  public Double getRequestRoutingStatsTimeoutS() {
    return requestRoutingStatsTimeoutS;
  }

  public void setRequestRoutingStatsPeriodS(Double requestRoutingStatsPeriodS) {
    this.requestRoutingStatsPeriodS = requestRoutingStatsPeriodS;
  }

  public void setRequestRoutingStatsTimeoutS(Double requestRoutingStatsTimeoutS) {
    this.requestRoutingStatsTimeoutS = requestRoutingStatsTimeoutS;
  }

  public io.ray.serve.generated.RequestRouterConfig toProto() {
    return io.ray.serve.generated.RequestRouterConfig.newBuilder()
        .setRequestRoutingStatsPeriodS(requestRoutingStatsPeriodS)
        .setRequestRoutingStatsTimeoutS(requestRoutingStatsTimeoutS)
        .build();
  }
}
