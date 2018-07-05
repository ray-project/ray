package org.ray.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

/**
 * Arguments for command submit
 */
@Parameters(separators = "= ", commandDescription = "submit a job to ray cluster")
public class CommandSubmit {

  // required
  @Parameter(names = "--package", description = "java jar package zip file")
  public String packageZip ;

  // required
  @Parameter(names = "--class", description = "java class name")
  public String className;

  // optional
  @Parameter(names = "--args", description = "arguments for the java class")
  public String classArgs;

  // optional
  @Parameter(names = "--config", description = "the config file of ray")
  public String config;

  // required
  @Parameter(names = "--redis-address", description = "ip & port for redis service")
  public String redis_address;

}
