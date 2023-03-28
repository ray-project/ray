package io.ray.serve.deployment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExampleEchoDeployment {
  String prefix;
  String suffix = "";
  private static final Logger LOGGER = LoggerFactory.getLogger(ExampleEchoDeployment.class);

  public ExampleEchoDeployment(Object prefix) {
    LOGGER.info("recieve init args: {}", prefix);
    this.prefix = (String) prefix;
  }

  public String call(Object input) {
    LOGGER.info("recieve call request: {}", input);
    return this.prefix + input + this.suffix;
  }

  public boolean checkHealth() {
    return true;
  }

  public Object reconfigure(Object userConfig) {
    LOGGER.info("recieve userconfig: {}", userConfig);
    if (null != userConfig) {
      this.suffix = userConfig.toString();
    }
    return null;
  }
}
