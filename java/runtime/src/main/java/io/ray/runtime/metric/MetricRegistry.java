package io.ray.runtime.metric;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MetricRegistry is a registry for metrics to be registered and updates metrics.
 */
public class MetricRegistry {

  public static final MetricRegistry DEFAULT_REGISTRY = new MetricRegistry();

  private static final Logger LOG = LoggerFactory.getLogger(MetricRegistry.class);

  private final Map<MetricId, Metric> registeredMetrics = new HashMap<>(64);

  private MetricConfig metricConfig;
  private ScheduledExecutorService scheduledExecutorService;
  private volatile boolean isRunning = false;

  public void startup() {
    startup(MetricConfig.DEFAULT_CONFIG);
  }

  public void startup(MetricConfig metricConfig) {
    synchronized (this) {
      if (!isRunning) {
        this.metricConfig = metricConfig;
        scheduledExecutorService = new ScheduledThreadPoolExecutor(metricConfig.threadPoolSize(),
          new ThreadFactoryBuilder().setNameFormat("metric-registry-%d").build());
        scheduledExecutorService.scheduleAtFixedRate(this::update, metricConfig.timeIntervalMs(),
          metricConfig.timeIntervalMs(), TimeUnit.MILLISECONDS);
        isRunning = true;
        LOG.info("Finished startup metric registry, metricConfig is {}.", metricConfig);
      }
    }
  }

  public void shutdown() {
    synchronized (this) {
      if (isRunning && scheduledExecutorService != null) {
        try {
          scheduledExecutorService.shutdownNow();
          if (!scheduledExecutorService
            .awaitTermination(metricConfig.shutdownWaitTimeMs(), TimeUnit.MILLISECONDS)) {
            LOG.warn("Metric registry did not shut down in {}ms time, so try to shut down again.",
              metricConfig.shutdownWaitTimeMs());
            scheduledExecutorService.shutdownNow();
          }
        } catch (InterruptedException e) {
          LOG.warn("Interrupted when shutting down metric registry, so try to shut down again.",
            e.getMessage(), e);
          scheduledExecutorService.shutdownNow();
        }
        if (scheduledExecutorService.isShutdown()) {
          isRunning = false;
          scheduledExecutorService = null;
          LOG.info("Metric registry has been shut down.");
        } else {
          LOG.warn("Failed to shut down metric registry service.");
        }
      }
    }
  }

  public Metric register(Metric metric) {
    synchronized (this) {
      if (!isRunning) {
        LOG.warn("Failed to register a metric, because the metric registry is not running.");
        return null;
      }
      try {
        MetricId id = genMetricIdByMetric(metric);
        Metric ori = registeredMetrics.putIfAbsent(id, metric);
        if (ori == null) {
          return metric;
        } else {
          LOG.info("Metric {} has already registered, so use the previous one.", id);
          return ori;
        }
      } catch (Exception e) {
        LOG.warn("Failed to register a metric: ", e.getMessage(), e);
        return null;
      }
    }
  }

  public void unregister(Metric metric) {
    synchronized (this) {
      if (!isRunning) {
        LOG.warn("Failed to unregister a metric, because the metric registry is not running.");
      }
      try {
        MetricId id = genMetricIdByMetric(metric);
        registeredMetrics.remove(id);
      } catch (Exception e) {
        LOG.warn("Failed to unregister a metric: ", e.getMessage(), e);
      }
    }
  }

  private void update() {
    registeredMetrics.forEach((id, metric) -> {
      metric.record();
    });
  }

  private MetricType getMetricType(Metric metric) {
    if (metric instanceof Count) {
      return MetricType.COUNT;
    }
    if (metric instanceof Gauge) {
      return MetricType.GAUGE;
    }
    if (metric instanceof Sum) {
      return MetricType.SUM;
    }
    if (metric instanceof Histogram) {
      return MetricType.HISTOGRAM;
    }
    throw new RuntimeException(
      "Unknown metric type, the metric is " + metric.getClass().getSimpleName());
  }

  private MetricId genMetricIdByMetric(Metric metric) {
    return new MetricId(getMetricType(metric), metric.name, metric.tags);
  }

}