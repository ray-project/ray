package org.ray.cli;

import com.beust.jcommander.Parameter;

/**
 * Arguments for Ray adm cli
 */
public class RayCliArgs {

  @Parameter(names = {"-h", "-help", "--help"}, description = "print this usage", help = true)
  public boolean help;
}
