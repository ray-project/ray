package org.ray.cli;

import com.beust.jcommander.JCommander;
import java.io.IOException;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.runner.RunManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Ray command line interface.
 */
public class RayCli {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayCli.class);

  private static RayCliArgs rayArgs = new RayCliArgs();

  private static RunManager startRayHead() {
    RayConfig rayConfig = RayConfig.create();
    RunManager manager = new RunManager(rayConfig);
    try {
      manager.startRayProcesses(true);
    } catch (Exception e) {
      LOGGER.error("Failed to start head node.", e);
      throw new RuntimeException("Failed to start Ray head node.", e);
    }
    LOGGER.info("Ray head node started. Redis address is {}", rayConfig.getRedisAddress());
    return manager;
  }

  private static RunManager startRayNode() {
    RayConfig rayConfig = RayConfig.create();
    RunManager manager = new RunManager(rayConfig);
    try {
      manager.startRayProcesses(false);
    } catch (Exception e) {
      LOGGER.error("Failed to start work node.", e);
      throw new RuntimeException("Failed to start work node.", e);
    }

    LOGGER.info("Ray work node started.");
    return manager;
  }

  private static RunManager startProcess(CommandStart cmdStart) {
    RunManager manager;
    if (cmdStart.head) {
      manager = startRayHead();
    } else {
      manager = startRayNode();
    }
    return manager;
  }

  private static void start(CommandStart cmdStart) {
    startProcess(cmdStart);
  }

  private static void stop(CommandStop cmdStop) {
    String[] cmd = {"/bin/sh", "-c", ""};
    cmd[2] = "kill $(ps aux | grep ray | grep -v grep | "
        + "awk \'{ print $2 }\') 2> /dev/null";
    try {
      Runtime.getRuntime().exec(cmd);
    } catch (IOException e) {
      LOGGER.error("Exception in killing ray processes.", e);
    }
  }

  public static void main(String[] args) {

    CommandStart cmdStart = new CommandStart();
    CommandStop cmdStop = new CommandStop();
    JCommander rayCommander = JCommander.newBuilder().addObject(rayArgs)
        .addCommand("start", cmdStart)
        .addCommand("stop", cmdStop)
        .build();
    rayCommander.parse(args);

    if (rayArgs.help) {
      rayCommander.usage();
      System.exit(0);
    }

    String cmd = rayCommander.getParsedCommand();
    if (cmd == null) {
      rayCommander.usage();
      System.exit(0);
    }

    switch (cmd) {
      case "start":
        start(cmdStart);
        break;
      case "stop":
        stop(cmdStop);
        break;
      default:
        rayCommander.usage();
    }
  }

}
