/**
 * Copyright 2015, Yahoo Inc. Licensed under the terms of the Apache License 2.0. Please see LICENSE
 * file in the project root for terms.
 */
package org.ray.streaming.benchmark.common.advertising;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUHashMap<K, V> extends LinkedHashMap<K, V> {

  private int cacheSize;

  public LRUHashMap(int cacheSize) {
    super(16, 0.75f, true);
    this.cacheSize = cacheSize;
  }

  protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
    return size() >= cacheSize;
  }
}
