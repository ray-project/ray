package io.ray.serve;

import com.google.common.collect.Lists;
import java.util.List;

/** Ray Serve common constants. */
public class Constants {

  /** Name of backend reconfiguration method implemented by user. */
  public static final String BACKEND_RECONFIGURE_METHOD = "reconfigure";

  /** Default histogram buckets for latency tracker. */
  public static final List<Double> DEFAULT_LATENCY_BUCKET_MS =
      Lists.newArrayList(
          1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0, 1000.0, 2000.0, 5000.0);

  /** Name of controller listen_for_change method. */
  public static final String CONTROLLER_LISTEN_FOR_CHANGE_METHOD = "listen_for_change";
}
