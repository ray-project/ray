package io.ray.runtime.runner;

import com.google.common.base.Joiner;
import io.ray.runtime.config.RayConfig;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Ray service management on one box. */
public class RunManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(RunManager.class);

  private static final Pattern pattern = Pattern.compile("--address='([^']+)'");

  /** Start the head node. */
  public static void startRayHead(RayConfig rayConfig) {
    LOGGER.debug("Starting ray runtime @ {}.", rayConfig.nodeIp);
    List<String> command = new ArrayList<>();
    command.add("ray");
    command.add("start");
    command.add("--head");
    command.add("--redis-username");
    command.add(rayConfig.redisUsername);
    command.add("--redis-password");
    command.add(rayConfig.redisPassword);
    command.addAll(rayConfig.headArgs);

    String output;
    try {
      output = runCommand(command);
    } catch (Exception e) {
      throw new RuntimeException("Failed to start Ray runtime.", e);
    }
    Matcher matcher = pattern.matcher(output);
    if (matcher.find()) {
      String bootstrapAddress = matcher.group(1);
      rayConfig.setBootstrapAddress(bootstrapAddress);
    } else {
      throw new RuntimeException("Redis address is not found. output: " + output);
    }
    LOGGER.info("Ray runtime started @ {}.", rayConfig.nodeIp);
  }

  /** Stop ray. */
  public static void stopRay() {
    List<String> command = new ArrayList<>();
    command.add("ray");
    command.add("stop");
    command.add("--force");

    try {
      runCommand(command);
    } catch (Exception e) {
      throw new RuntimeException("Failed to stop ray.", e);
    }
  }

  /**
   * Start a process.
   *
   * @param command The command to start the process with.
   */
  public static String runCommand(List<String> command) throws IOException, InterruptedException {
    return runCommand(command, 30, TimeUnit.SECONDS);
  }

  public static String runCommand(List<String> command, long timeout, TimeUnit unit)
      throws IOException, InterruptedException {
    LOGGER.info("Starting process with command: {}", Joiner.on(" ").join(command));

    // A pipe can fill while waitFor is blocking, preventing the child from exiting.
    Path outputFile = Files.createTempFile("ray-command-", ".log");
    Process p = null;
    try {
      ProcessBuilder builder =
          new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(outputFile.toFile());
      p = builder.start();
      final boolean exited = p.waitFor(timeout, unit);
      if (!exited) {
        p.destroyForcibly();
        p.waitFor(5, TimeUnit.SECONDS);
      }
      String output = new String(Files.readAllBytes(outputFile), Charset.defaultCharset());
      if (!exited) {
        throw new RuntimeException("The process was not exited in time. output:\n" + output);
      }
      if (p.exitValue() != 0) {
        String sb =
            "The exit value of the process is "
                + p.exitValue()
                + ". Command: "
                + Joiner.on(" ").join(command)
                + "\n"
                + "output:\n"
                + output;
        throw new RuntimeException(sb);
      }
      return output;
    } finally {
      if (p != null && p.isAlive()) {
        p.destroyForcibly();
      }
      try {
        Files.deleteIfExists(outputFile);
      } catch (IOException e) {
        LOGGER.warn("Failed to delete command output file {}", outputFile, e);
      }
    }
  }
}
