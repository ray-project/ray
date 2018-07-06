package org.ray.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

/**
 * Arguments for command submit
 */
@Parameters(separators = "= ", commandDescription = "submit a job to ray cluster")
public class CommandSubmit {

  @Parameter(names = "--package", description = "java jar package zip file", required = true)
  public String packageZip ;

  @Parameter(names = "--class", description = "java class name", required = true)
  public String className;

  @Parameter(names = "--args", description = "arguments for the java class")
  public String classArgs;

  @Parameter(names = "--config", description = "the config file of ray")
  public String config;

  @Parameter(names = "--redis-address", description = "ip & port for redis service", required = true)
  public String redis_address;

}
