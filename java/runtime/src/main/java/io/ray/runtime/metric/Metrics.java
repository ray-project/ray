package io.ray.runtime.metric;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The entry of metrics for easy use.
 */
public final class Metrics {

  private static MetricRegistry metricRegistry;

  public static MetricRegistry init(MetricConfig metricConfig) {
    synchronized (Metrics.class) {
      metricRegistry = new MetricRegistry();
      metricRegistry.startup(metricConfig);
      return metricRegistry;
    }
  }

  public static void shutdown() {
    synchronized (Metrics.class) {
      if (metricRegistry != null) {
        metricRegistry.shutdown();
        metricRegistry = null;
      }
    }
  }

  public static CountBuilder count() {
    return new CountBuilder();
  }

  public static GaugeBuilder gauge() {
    return new GaugeBuilder();
  }

  public static SumBuilder sum() {
    return new SumBuilder();
  }

  public static HistogramBuilder histogram() {
    return new HistogramBuilder();
  }

  public static class CountBuilder extends AbstractBuilder<CountBuilder, Count> {

    @Override
    protected Count create() {
      return new Count(name, description, unit, generateTagKeysMap(tags));
    }
  }

  public static class GaugeBuilder extends AbstractBuilder<GaugeBuilder, Gauge> {

    @Override
    protected Gauge create() {
      return new Gauge(name, description, unit, generateTagKeysMap(tags));
    }
  }

  public static class SumBuilder extends AbstractBuilder<SumBuilder, Sum> {

    @Override
    protected Sum create() {
      return new Sum(name, description, unit, generateTagKeysMap(tags));
    }
  }

  public static class HistogramBuilder extends AbstractBuilder<HistogramBuilder, Histogram> {

    private List<Double> boundaries;

    public HistogramBuilder boundaries(List<Double> boundaries) {
      this.boundaries = boundaries;
      return this;
    }

    @Override
    protected Histogram create() {
      return new Histogram(name, description, unit, boundaries, generateTagKeysMap(tags));
    }
  }

  public abstract static class AbstractBuilder<B extends AbstractBuilder, M extends Metric> {
    protected String name;
    protected String description;
    protected String unit;
    protected Map<String, String> tags;

    public B name(String name) {
      this.name = Preconditions.checkNotNull(name);
      return (B) this;
    }

    public B description(String description) {
      this.description = Preconditions.checkNotNull(description);
      return (B) this;
    }

    public B unit(String unit) {
      this.unit = Preconditions.checkNotNull(unit);
      return (B) this;
    }

    public B tags(Map<String, String> tags) {
      this.tags = Preconditions.checkNotNull(tags);
      return (B) this;
    }

    /**
     * Creates a metric by sub-class.
     *
     * @return a metric
     */
    protected abstract M create();

    public M register() {
      M m = create();
      maybeInitRegistry();
      return (M) metricRegistry.register(m);
    }
  }

  private static Map<TagKey, String> generateTagKeysMap(Map<String, String> tags) {
    Map<TagKey, String> tagKeys = new HashMap<>(tags.size() * 2);
    tags.forEach((key, value) -> {
      TagKey tagKey = new TagKey(key);
      tagKeys.put(tagKey, value);
    });
    return tagKeys;
  }

  private static MetricRegistry maybeInitRegistry() {
    synchronized (Metrics.class) {
      if (metricRegistry != null) {
        return metricRegistry;
      } else {
        metricRegistry = MetricRegistry.DEFAULT_REGISTRY;
        metricRegistry.startup();
        return metricRegistry;
      }
    }
  }

}