package io.ray.streaming.runtime.util;

import io.ray.runtime.util.BinaryFileUtil;
import io.ray.runtime.util.JniUtils;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnvUtil {

  private static final Logger LOG = LoggerFactory.getLogger(EnvUtil.class);

  public static String getJvmPid() {
    return ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
  }

  public static String getHostName() {
    String hostname = "";
    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.error("Error occurs while fetching local host.", e);
    }
    return hostname;
  }

  public static void loadNativeLibraries() {
    JniUtils.loadLibrary(BinaryFileUtil.CORE_WORKER_JAVA_LIBRARY, true);
    JniUtils.loadLibrary("streaming_java");
  }

  /**
   * Execute an external command.
   *
   * <p>Returns Whether the command succeeded.
   */
  public static boolean executeCommand(List<String> command, int waitTimeoutSeconds) {
    try {
      ProcessBuilder processBuilder =
          new ProcessBuilder(command)
              .redirectOutput(ProcessBuilder.Redirect.INHERIT)
              .redirectError(ProcessBuilder.Redirect.INHERIT);
      Process process = processBuilder.start();
      boolean exit = process.waitFor(waitTimeoutSeconds, TimeUnit.SECONDS);
      if (!exit) {
        process.destroyForcibly();
      }
      return process.exitValue() == 0;
    } catch (Exception e) {
      throw new RuntimeException("Error executing command " + String.join(" ", command), e);
    }
  }
}
