package io.ray.streaming.runtime.util;

import com.sun.management.OperatingSystemMXBean;
import io.ray.streaming.runtime.core.resource.Container;
import io.ray.streaming.runtime.core.resource.ContainerId;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Resource Utility collects current OS and JVM resource usage information */
public class ResourceUtil {

  public static final Logger LOG = LoggerFactory.getLogger(ResourceUtil.class);

  /**
   * Refer to:
   * https://docs.oracle.com/javase/8/docs/jre/api/management/extension/com/sun/management/OperatingSystemMXBean.html
   */
  private static OperatingSystemMXBean osmxb =
      (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

  /** Log current jvm process's memory detail */
  public static void logProcessMemoryDetail() {
    int mb = 1024 * 1024;

    // Getting the runtime reference from system
    Runtime runtime = Runtime.getRuntime();

    StringBuilder sb = new StringBuilder(32);

    sb.append("used memory: ")
        .append((runtime.totalMemory() - runtime.freeMemory()) / mb)
        .append(", free memory: ")
        .append(runtime.freeMemory() / mb)
        .append(", total memory: ")
        .append(runtime.totalMemory() / mb)
        .append(", max memory: ")
        .append(runtime.maxMemory() / mb);

    if (LOG.isInfoEnabled()) {
      LOG.info(sb.toString());
    }
  }

  /**
   * Returns jvm heap usage ratio. note that one of the survivor space is not include in total
   * memory while calculating this ratio.
   */
  public static double getJvmHeapUsageRatio() {
    Runtime runtime = Runtime.getRuntime();
    return (runtime.totalMemory() - runtime.freeMemory()) * 1.0 / runtime.maxMemory();
  }

  /**
   * Returns jvm heap usage(in bytes). note that this value doesn't include one of the survivor
   * space.
   */
  public static long getJvmHeapUsageInBytes() {
    Runtime runtime = Runtime.getRuntime();
    return runtime.totalMemory() - runtime.freeMemory();
  }

  /** Returns the total amount of physical memory in bytes. */
  public static long getSystemTotalMemory() {
    return osmxb.getTotalPhysicalMemorySize();
  }

  /** Returns the used system physical memory in bytes */
  public static long getSystemMemoryUsage() {
    long totalMemory = osmxb.getTotalPhysicalMemorySize();
    long freeMemory = osmxb.getFreePhysicalMemorySize();
    return totalMemory - freeMemory;
  }

  /** Returns the ratio of used system physical memory. This value is a double in the [0.0,1.0] */
  public static double getSystemMemoryUsageRatio() {
    double totalMemory = osmxb.getTotalPhysicalMemorySize();
    double freeMemory = osmxb.getFreePhysicalMemorySize();
    double ratio = freeMemory / totalMemory;
    return 1 - ratio;
  }

  /** Returns the cpu load for current jvm process. This value is a double in the [0.0,1.0] */
  public static double getProcessCpuUsage() {
    return osmxb.getProcessCpuLoad();
  }

  /**
   * Returns the system cpu usage. This value is a double in the [0.0,1.0] We will try to use `vsar`
   * to get cpu usage by default, and use MXBean if any exception raised.
   */
  public static double getSystemCpuUsage() {
    double cpuUsage = 0.0;
    try {
      cpuUsage = getSystemCpuUtilByVsar();
    } catch (Exception e) {
      cpuUsage = getSystemCpuUtilByMXBean();
    }
    return cpuUsage;
  }

  /**
   * Returns the "recent cpu usage" for the whole system. This value is a double in the [0.0,1.0]
   * interval. A value of 0.0 means that all CPUs were idle during the recent period of time
   * observed, while a value of 1.0 means that all CPUs were actively running 100% of the time
   * during the recent period being observed
   */
  public static double getSystemCpuUtilByMXBean() {
    return osmxb.getSystemCpuLoad();
  }

  /** Get system cpu util by vsar */
  public static double getSystemCpuUtilByVsar() throws Exception {
    double cpuUsageFromVsar = 0.0;
    String[] vsarCpuCommand = {"/bin/sh", "-c", "vsar --check --cpu -s util"};
    try {
      Process proc = Runtime.getRuntime().exec(vsarCpuCommand);
      BufferedInputStream bis = new BufferedInputStream(proc.getInputStream());
      BufferedReader br = new BufferedReader(new InputStreamReader(bis));
      String line;
      List<String> processPidList = new ArrayList<>();
      while ((line = br.readLine()) != null) {
        processPidList.add(line);
      }
      if (!processPidList.isEmpty()) {
        String[] split = processPidList.get(0).split("=");
        cpuUsageFromVsar = Double.parseDouble(split[1]) / 100.0D;
      } else {
        throw new IOException("Vsar check cpu usage failed, maybe vsar is not installed.");
      }
    } catch (Exception e) {
      LOG.warn("Failed to get cpu usage by vsar.", e);
      throw e;
    }
    return cpuUsageFromVsar;
  }

  /** Returnss the system load average for the last minute */
  public static double getSystemLoadAverage() {
    return osmxb.getSystemLoadAverage();
  }

  /** Returns system cpu cores num */
  public static int getCpuCores() {
    return osmxb.getAvailableProcessors();
  }

  /**
   * Get containers by hostname of address
   *
   * @param containers container list
   * @param containerHosts container hostname or address set Returns matched containers
   */
  public static List<Container> getContainersByHostname(
      List<Container> containers, Collection<String> containerHosts) {

    return containers.stream()
        .filter(
            container ->
                containerHosts.contains(container.getHostname())
                    || containerHosts.contains(container.getAddress()))
        .collect(Collectors.toList());
  }

  /**
   * Get container by hostname
   *
   * @param hostName container hostname Returns container
   */
  public static Optional<Container> getContainerByHostname(
      List<Container> containers, String hostName) {
    return containers.stream()
        .filter(
            container ->
                container.getHostname().equals(hostName) || container.getAddress().equals(hostName))
        .findFirst();
  }

  /**
   * Get container by id
   *
   * @param containerID container id Returns container
   */
  public static Optional<Container> getContainerById(
      List<Container> containers, ContainerId containerID) {
    return containers.stream()
        .filter(container -> container.getId().equals(containerID))
        .findFirst();
  }
}
