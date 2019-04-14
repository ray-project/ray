package org.ray.api.gcs;

import java.util.List;

/**
 * The client used to interface with the GCS.
 */
public interface GcsClient {

  /**
   * Get all node information in Ray cluster.
   */
  List<NodeInfo> getAllNodeInfo();

}
