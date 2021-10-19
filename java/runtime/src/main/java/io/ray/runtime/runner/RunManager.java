package io.ray.runtime.runner;

import com.google.common.base.Joiner;
import io.ray.runtime.config.RayConfig;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.IOUtils;
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
    command.add("--redis-password");
    command.add(rayConfig.redisPassword);
    command.addAll(rayConfig.headArgs);

    String numGpus = System.getProperty("num-gpus");
    if (numGpus != null) {
      command.add("--num-gpus");
      command.add(numGpus);
    }

    String output;
    try {
      output = runCommand(command);
    } catch (Exception e) {
      throw new RuntimeException("Failed to start Ray runtime.", e);
    }
    Matcher matcher = pattern.matcher(output);
    if (matcher.find()) {
      String redisAddress = matcher.group(1);
      rayConfig.setRedisAddress(redisAddress);
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
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Starting process with command: {}", Joiner.on(" ").join(command));
    }

    ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true);
    Process p = builder.start();
    String output = IOUtils.toString(p.getInputStream(), Charset.defaultCharset());
    p.waitFor();
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
  }
}
