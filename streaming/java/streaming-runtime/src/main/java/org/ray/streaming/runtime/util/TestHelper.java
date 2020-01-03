package org.ray.streaming.runtime.util;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.ray.api.id.UniqueId;
import org.ray.api.runtimecontext.NodeInfo;

public class TestHelper {

  private static final String UT_PATTERN = "UT_PATTERN";

  public static final String MASTER_ACTOR_PATH_PREFIX = "/tmp/masterActor_";

  public static void setUTPattern() {
    System.setProperty(UT_PATTERN, "");
  }

  public static void clearUTPattern() {
    System.clearProperty(UT_PATTERN);
  }

  public static boolean isUTPattern() {
    return System.getProperty(UT_PATTERN) != null;
  }

  public static String getMasterActorFilePath(String jobName) {
    return MASTER_ACTOR_PATH_PREFIX + jobName;
  }

  public static boolean checkMasterActorFileExist(String jobName) {
    File file = new File(getMasterActorFilePath(jobName));
    return file.exists() && file.isFile();
  }

  public static void deleteMasterActorFile(String jobName) {
    File file = new File(getMasterActorFilePath(jobName));
    if (file.exists() && file.isFile()) {
      file.delete();
    }
  }

  public static Map<String, UniqueId> mockNodeName2Ids(List<NodeInfo> nodeInfos) {
    Map<String, UniqueId> nodeName2Ids = new HashMap<>();
    for (NodeInfo nodeInfo : nodeInfos) {
      nodeName2Ids.put(nodeInfo.nodeAddress, nodeInfo.nodeId);
    }
    return nodeName2Ids;
  }

  public static List<NodeInfo> mockContainerResources() {
    List<NodeInfo> nodeInfos = new LinkedList<>();

    for (int i = 1; i <= 5; i++) {
      Map<String, Double> resources = new HashMap<>();
      resources.put("MEM", 16.0);
      switch (i) {
        case 1:
          resources.put("CPU", 3.0);
          break;
        case 2:
        case 3:
        case 4:
          resources.put("CPU", 4.0);
          break;
        case 5:
          resources.put("CPU", 2.0);
          break;
        default:
          resources.put("CPU", 1.0);
      }

      byte[] nodeIdBytes = new byte[UniqueId.LENGTH];
      for (int byteIndex = 0; byteIndex < UniqueId.LENGTH; ++byteIndex) {
        nodeIdBytes[byteIndex] = String.valueOf(i).getBytes()[0];
      }
      NodeInfo nodeInfo = new NodeInfo(new UniqueId(nodeIdBytes),
          "localhost" + i, "localhost" + i,
          true, resources);
      nodeInfos.add(nodeInfo);
    }
    return nodeInfos;
  }
}