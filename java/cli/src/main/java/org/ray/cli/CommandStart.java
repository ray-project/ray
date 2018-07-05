package org.ray.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

/**
 * Arguments for command start
 */
@Parameters(separators = "= ", commandDescription = "start ray daemons")
public class CommandStart {

  @Parameter(names = "--head", description = "start the head node")
  public boolean head;

  @Parameter(names = "--config", description = "the config file of ray")
  public String config = "";

  @Parameter(names = "--overwrite", description = "the overwrite items of config")
  public String overwrite = "";

}
