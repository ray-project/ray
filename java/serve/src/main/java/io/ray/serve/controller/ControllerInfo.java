package io.ray.serve.controller;

import java.io.Serializable;

public class ControllerInfo implements Serializable {

  private static final long serialVersionUID = 3511790838979680633L;

  private String controllerName;

  private String controllerNamespace;

  public ControllerInfo(String controllerName, String controllerNamespace) {
    this.controllerName = controllerName;
    this.controllerNamespace = controllerNamespace;
  }

  public String getControllerName() {
    return controllerName;
  }

  public String getControllerNamespace() {
    return controllerNamespace;
  }
}
