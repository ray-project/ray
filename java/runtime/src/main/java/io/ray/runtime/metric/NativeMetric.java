package io.ray.runtime.metric;

import java.util.List;

/**
 * Native metric provide a native interface to register tag or metric for current metric package.
 */
class NativeMetric {
  public static native void registerTagkeyNative(String tagKey);

  public static native long registerCountNative(
      String name, String description, String unit, List<String> tagKeys);

  public static native long registerGaugeNative(
      String name, String description, String unit, List<String> tagKeys);

  public static native long registerHistogramNative(
      String name, String description, String unit, double[] boundaries, List<String> tagKeys);

  public static native long registerSumNative(
      String name, String description, String unit, List<String> tagKeys);

  public static native void recordNative(
      long metricNativePointer, double value, List tagKeys, List<String> tagValues);

  public static native void unregisterMetricNative(long gaugePtr);
}
