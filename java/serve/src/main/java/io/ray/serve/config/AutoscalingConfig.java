package io.ray.serve.config;

import java.io.Serializable;

public class AutoscalingConfig implements Serializable {
  private static final long serialVersionUID = 9135422781025005216L;
  private int minReplicas = 1;
  private int maxReplicas = 1;
  private int targetNumOngoingRequestsPerReplica = 1;
  /** How often to scrape for metrics */
  private double metricsIntervalS = 10.0;
  /** Time window to average over for metrics. */
  private double lookBackPeriodS = 30.0;
  /** Multiplicative "gain" factor to limit scaling decisions */
  private double smoothingFactor = 1.0;
  /** How long to wait before scaling down replicas */
  private double downscaleDelayS = 600.0;
  /** How long to wait before scaling up replicas */
  private double upscaleDelayS = 30.0;

  public int getMinReplicas() {
    return minReplicas;
  }

  public void setMinReplicas(int minReplicas) {
    this.minReplicas = minReplicas;
  }

  public int getMaxReplicas() {
    return maxReplicas;
  }

  public void setMaxReplicas(int maxReplicas) {
    this.maxReplicas = maxReplicas;
  }

  public int getTargetNumOngoingRequestsPerReplica() {
    return targetNumOngoingRequestsPerReplica;
  }

  public void setTargetNumOngoingRequestsPerReplica(int targetNumOngoingRequestsPerReplica) {
    this.targetNumOngoingRequestsPerReplica = targetNumOngoingRequestsPerReplica;
  }

  public double getMetricsIntervalS() {
    return metricsIntervalS;
  }

  public void setMetricsIntervalS(double metricsIntervalS) {
    this.metricsIntervalS = metricsIntervalS;
  }

  public double getLookBackPeriodS() {
    return lookBackPeriodS;
  }

  public void setLookBackPeriodS(double lookBackPeriodS) {
    this.lookBackPeriodS = lookBackPeriodS;
  }

  public double getSmoothingFactor() {
    return smoothingFactor;
  }

  public void setSmoothingFactor(double smoothingFactor) {
    this.smoothingFactor = smoothingFactor;
  }

  public double getDownscaleDelayS() {
    return downscaleDelayS;
  }

  public void setDownscaleDelayS(double downscaleDelayS) {
    this.downscaleDelayS = downscaleDelayS;
  }

  public double getUpscaleDelayS() {
    return upscaleDelayS;
  }

  public void setUpscaleDelayS(double upscaleDelayS) {
    this.upscaleDelayS = upscaleDelayS;
  }

  public io.ray.serve.generated.AutoscalingConfig toProto() {
    return io.ray.serve.generated.AutoscalingConfig.newBuilder()
        .setMinReplicas(minReplicas)
        .setMaxReplicas(maxReplicas)
        .setTargetNumOngoingRequestsPerReplica(targetNumOngoingRequestsPerReplica)
        .setMetricsIntervalS(metricsIntervalS)
        .setLookBackPeriodS(lookBackPeriodS)
        .setSmoothingFactor(smoothingFactor)
        .setDownscaleDelayS(downscaleDelayS)
        .setUpscaleDelayS(upscaleDelayS)
        .build();
  }
}
