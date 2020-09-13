package io.ray.runtime.runner;

import com.google.common.base.Joiner;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.ray.runtime.config.RayConfig;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ray service management on one box.
 */
public class RunManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(RunManager.class);

  private static final Pattern pattern = Pattern.compile("--address='([^']+)'");

  /**
   * Start the head node.
   */
  public static void startRayHead(RayConfig rayConfig) {
    List<String> command = Arrays.asList(
      "ray",
      "start",
      "--head",
      "--system-config=" + new Gson().toJson(rayConfig.rayletConfigParameters),
      "--code-search-path=" + System.getProperty("java.class.path")
    );
    String output;
    try {
      output = runCommand(command);
    } catch (Exception e) {
      throw new RuntimeException("Failed to start ray.", e);
    }
    Matcher matcher = pattern.matcher(output);
    if (matcher.find()) {
      String redisAddress = matcher.group(1);
      rayConfig.setRedisAddress(redisAddress);
    } else {
      throw new RuntimeException("Redis address is not found. output: " + output);
    }
  }

  /**
   * Stop ray.
   */
  public static void stopRay() {
    List<String> command = new ArrayList<>();
    command.add("ray");
    command.add("stop");

    try {
      runCommand(command);
    } catch (Exception e) {
      throw new RuntimeException("Failed to stop ray.", e);
    }
  }

  public static void fillConfigForDriver(RayConfig rayConfig) {
    List<String> command = Arrays.asList(
      "python",
      "-c",
      String.format(
        "import ray; print(ray.services.get_address_info_from_redis('%s', '%s', redis_password='%s'))",
        rayConfig.getRedisAddress(), rayConfig.nodeIp, rayConfig.redisPassword)
    );

    String output;
    try {
      output = runCommand(command);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get address info.", e);
    }

    JsonObject addressInfo = new JsonParser().parse(output).getAsJsonObject();
    rayConfig.rayletSocketName = addressInfo.get("raylet_socket_name").getAsString();
    rayConfig.objectStoreSocketName = addressInfo.get("object_store_address").getAsString();
    rayConfig.nodeManagerPort = addressInfo.get("node_manager_port").getAsInt();
  }

  /**
   * Start a process.
   *
   * @param command The command to start the process with.
   */
  private static String runCommand(List<String> command) throws IOException, InterruptedException {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Starting process with command: {}", Joiner.on(" ").join(command));
    }

    ProcessBuilder builder = new ProcessBuilder(command);

    Process p = builder.start();

    p.waitFor();

    String stdout = IOUtils.toString(p.getInputStream(), Charset.defaultCharset());
    String stderr = IOUtils.toString(p.getErrorStream(), Charset.defaultCharset());
    if (p.exitValue() != 0) {
      String sb = "The exit value of the process is " + p.exitValue()
        + ". Command: " + Joiner.on(" ").join(command)
        + "\n"
        + "stdout:\n"
        + stdout
        + "stderr:\n"
        + stderr;
      throw new RuntimeException(sb);
    }
    return stdout;
  }
}
