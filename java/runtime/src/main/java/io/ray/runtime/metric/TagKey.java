package io.ray.runtime.metric;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Tagkey is mapping java object to stats tagkey object. */
public class TagKey {

  private String tagKey;

  public TagKey(String key) {
    tagKey = key;
    NativeMetric.registerTagkeyNative(key);
  }

  public String getTagKey() {
    return tagKey;
  }

  /**
   * Convert pair of string and string map to tags map that can be recognized in native layer.
   *
   * @param tags metrics key value map.
   * @return
   */
  public static Map<TagKey, String> tagsFromMap(Map<String, String> tags) {
    Map<TagKey, String> tagKeyMap = new HashMap<>();
    for (Map.Entry<String, String> entry : tags.entrySet()) {
      tagKeyMap.put(new TagKey(entry.getKey()), entry.getValue());
    }
    return tagKeyMap;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TagKey)) {
      return false;
    }
    TagKey tagKey1 = (TagKey) o;
    return Objects.equals(tagKey, tagKey1.tagKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tagKey);
  }

  @Override
  public String toString() {
    return "TagKey{" + ", tagKey='" + tagKey + '\'' + '}';
  }
}
