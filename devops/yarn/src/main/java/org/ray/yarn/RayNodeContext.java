package org.ray.yarn;

import org.apache.hadoop.yarn.api.records.Container;

public class RayNodeContext {

  RayNodeContext(String role) {
    this.role = role;
  }

  String role;

  boolean isRunning = false;

  boolean isAlocating = false;

  String instanceId = null;

  Container container = null;

  int failCounter = 0;
}
