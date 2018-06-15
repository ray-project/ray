package org.ray.yarn;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Constants used in both Client and Application Master.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class DsConstants {

  /**
   * Environment key name pointing to the shell script's location.
   */
  public static final String RAY_ARCHIVE_LOCATION = "RAY_ARCHIVE_LOCATION";

  /**
   * Environment key name denoting the file timestamp for the shell script. Used to validate the
   * local resource.
   */
  public static final String RAY_ARCHIVE_TIMESTAMP = "RAY_ARCHIVE_TIMESTAMP";

  /**
   * Environment key name denoting the file content length for the shell script. Used to validate
   * the local resource.
   */
  public static final String RAY_ARCHIVE_LEN = "RAY_ARCHIVE_LEN";

  /**
   * Environment key name denoting the timeline domain ID.
   */
  public static final String RAY_TIMELINE_DOMAIN = "RAY_TIMELINE_DOMAIN";
}
