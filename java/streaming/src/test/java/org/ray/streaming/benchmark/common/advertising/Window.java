/**
 * Copyright 2015, Yahoo Inc. Licensed under the terms of the Apache License 2.0. Please see LICENSE
 * file in the project root for terms.
 */
package org.ray.streaming.benchmark.common.advertising;

public class Window {

  public String timestamp;
  public Long seenCount;

  @Override
  public boolean equals(Object other) {
    if (other instanceof Window) {
      return timestamp.equals(((Window) other).timestamp);
    }
    return false;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;

    result = result * prime + timestamp.hashCode();

    return result;
  }

  @Override
  public String toString() {
    return "{ time: " + timestamp + ", seen: " + seenCount + " }";
  }
}
