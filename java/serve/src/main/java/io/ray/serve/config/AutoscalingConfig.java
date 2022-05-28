package io.ray.serve.config;

import java.io.Serializable;

public class AutoscalingConfig implements Serializable {

  private static final long serialVersionUID = 9135422781025005216L;

  private int minReplicas;

  private int maxReplicas;

  private int targetNumOngoingRequestsPerReplica;

  private double metricsIntervalS;

  private double lookBackPeriodS;

  private double smoothingFactor;

  private double downscaleDelayS;

  private double upscaleDelayS;

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
