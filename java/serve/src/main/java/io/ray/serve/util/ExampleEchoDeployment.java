package io.ray.serve.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExampleEchoDeployment {
  String prefix;
  private static final Logger LOGGER = LoggerFactory.getLogger(ExampleEchoDeployment.class);

  public ExampleEchoDeployment(Object prefix) {
    this.prefix = (String) prefix;
  }

  public String call(Object input) {
    return this.prefix + (String) input;
  }

  public boolean checkHealth() {
    return true;
  }
}
