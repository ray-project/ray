package org.ray.runner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.ray.spi.model.AddressInfo;

/**
 * information of kinds of processes.
 */
public class RunInfo {

  public String redisAddress;
  public List<String> redisShards;
  public List<AddressInfo> localStores = new ArrayList<>();
  public ArrayList<List<ProcessInfo>> allProcesses = initProcessInfoArray();
  public ArrayList<List<Process>> toBeCleanedProcesses = initProcessArray();
  public ArrayList<ProcessInfo> deadProcess = new ArrayList<>();

  private ArrayList<List<Process>> initProcessArray() {
    ArrayList<List<Process>> processes = new ArrayList<>();
    for (ProcessType ignored : ProcessType.values()) {
      processes.add(Collections.synchronizedList(new ArrayList<>()));
    }
    return processes;
  }

  private ArrayList<List<ProcessInfo>> initProcessInfoArray() {
    ArrayList<List<ProcessInfo>> processes = new ArrayList<>();
    for (ProcessType ignored : ProcessType.values()) {
      processes.add(Collections.synchronizedList(new ArrayList<>()));
    }
    return processes;
  }

  public enum ProcessType { PT_WORKER, PT_PLASMA_STORE,
    PT_REDIS_SERVER, PT_WEB_UI, PT_RAYLET, PT_DRIVER
  }
}
