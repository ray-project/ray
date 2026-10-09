package io.ray.runtime.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ResourceUtil {

  /**
   * Get the device IDs in the CUDA_VISIBLE_DEVICES environment variable. The local mode is not
   * support.
   *
   * @return devices (List[String]): If CUDA_VISIBLE_DEVICES is set, returns a list of strings
   *     representing the IDs of the visible GPUs. If it is not set or is set to NoDevFiles, returns
   *     empty list.
   */
  public static List<String> getCudaVisibleDevices() {
    List<String> gpuDevices = new ArrayList<>();
    String gpuIdsStr = System.getenv("CUDA_VISIBLE_DEVICES");
    if (gpuIdsStr == null) {
      return null;
    } else if (gpuIdsStr.isEmpty()) {
      return gpuDevices;
    } else if ("NoDevFiles".equals(gpuIdsStr)) {
      return gpuDevices;
    }
    gpuDevices = Arrays.stream(gpuIdsStr.split(",")).collect(Collectors.toList());
    return gpuDevices;
  }
}
