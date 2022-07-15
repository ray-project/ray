package io.ray.runtime.metric;

import com.google.common.base.MoreObjects;

/** Configurations of the metric. */
public class MetricConfig {

  private static final long DEFAULT_TIME_INTERVAL_MS = 5000L;
  private static final int DEFAULT_THREAD_POLL_SIZE = 1;
  private static final long DEFAULT_SHUTDOWN_WAIT_TIME_MS = 3000L;

  public static final MetricConfig DEFAULT_CONFIG =
      new MetricConfig(
          DEFAULT_TIME_INTERVAL_MS, DEFAULT_THREAD_POLL_SIZE, DEFAULT_SHUTDOWN_WAIT_TIME_MS);

  private final long timeIntervalMs;
  private final int threadPoolSize;
  private final long shutdownWaitTimeMs;

  public MetricConfig(long timeIntervalMs, int threadPoolSize, long shutdownWaitTimeMs) {
    this.timeIntervalMs = timeIntervalMs;
    this.threadPoolSize = threadPoolSize;
    this.shutdownWaitTimeMs = shutdownWaitTimeMs;
  }

  public long timeIntervalMs() {
    return timeIntervalMs;
  }

  public int threadPoolSize() {
    return threadPoolSize;
  }

  public long shutdownWaitTimeMs() {
    return shutdownWaitTimeMs;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("timeIntervalMs", timeIntervalMs)
        .add("threadPoolSize", threadPoolSize)
        .add("shutdownWaitTimeMs", shutdownWaitTimeMs)
        .toString();
  }

  public static MetricConfigBuilder builder() {
    return new MetricConfigBuilder();
  }

  public static class MetricConfigBuilder {
    private long timeIntervalMs = DEFAULT_TIME_INTERVAL_MS;
    private int threadPooSize = DEFAULT_THREAD_POLL_SIZE;
    private long shutdownWaitTimeMs = DEFAULT_SHUTDOWN_WAIT_TIME_MS;

    public MetricConfig create() {
      return new MetricConfig(timeIntervalMs, threadPooSize, shutdownWaitTimeMs);
    }

    public MetricConfigBuilder timeIntervalMs(long timeIntervalMs) {
      this.timeIntervalMs = timeIntervalMs;
      return this;
    }

    public MetricConfigBuilder threadPoolSize(int threadPooSize) {
      this.threadPooSize = threadPooSize;
      return this;
    }

    public MetricConfigBuilder shutdownWaitTimeMs(long shutdownWaitTimeMs) {
      this.shutdownWaitTimeMs = shutdownWaitTimeMs;
      return this;
    }
  }
}
