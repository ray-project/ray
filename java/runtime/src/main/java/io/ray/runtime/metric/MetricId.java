package io.ray.runtime.metric;

import com.google.common.base.MoreObjects;
import java.util.Map;
import java.util.Objects;

/**
 * MetricId represents a metric with a given type, name and tags.
 * If two metrics have the same type and name but different tags(including key and value), they have
 * a different MetricId. And in this way, {@link MetricRegistry} can register two metrics with same
 * name but different tags.
 */
public class MetricId {

  private final MetricType type;
  private final String name;
  private final Map<TagKey, String> tags;

  public MetricId(MetricType type, String name, Map<TagKey, String> tags) {
    this.type = type;
    this.name = name;
    this.tags = tags;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MetricId)) {
      return false;
    }
    MetricId metricId = (MetricId) o;
    return type == metricId.type &&
      Objects.equals(name, metricId.name) &&
      Objects.equals(tags, metricId.tags);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, name, tags);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("type", type)
      .add("name", name)
      .add("tags", tags)
      .toString();
  }

  public MetricType getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  public Map<TagKey, String> getTags() {
    return tags;
  }
}