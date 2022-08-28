package io.ray.serve.common;

import com.google.common.collect.Lists;
import java.util.List;

/** Ray Serve common constants. */
public class Constants {

  /** Name of deployment reconfiguration method implemented by user. */
  public static final String RECONFIGURE_METHOD = "reconfigure";

  /** Default histogram buckets for latency tracker. */
  public static final List<Double> DEFAULT_LATENCY_BUCKET_MS =
      Lists.newArrayList(
          1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0, 1000.0, 2000.0, 5000.0);

  /** Name of controller listen_for_change method. */
  public static final String CONTROLLER_LISTEN_FOR_CHANGE_METHOD = "listen_for_change_java";

  /** Name of controller get_all_endpoints method. */
  public static final String CONTROLLER_GET_ALL_ENDPOINTS_METHOD = "get_all_endpoints_java";

  public static final String SERVE_CONTROLLER_NAME = "SERVE_CONTROLLER_ACTOR";

  public static final String SERVE_NAMESPACE = "serve";

  public static final String CALL_METHOD = "call";

  public static final String UTF8 = "UTF-8";

  public static final String CHECK_HEALTH_METHOD = "checkHealth";

  /** Controller checkpoint path */
  public static final String DEFAULT_CHECKPOINT_PATH = "ray://";

  /**
   * Because ServeController will accept one long poll request per handle, its concurrency needs to
   * scale as O(num_handles)
   */
  public static final int CONTROLLER_MAX_CONCURRENCY = 15000;

  /** Max time to wait for proxy in `Serve.start`. Unit: second */
  public static final int PROXY_TIMEOUT_S = 60;

  public static final Double DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_S = 20.0;

  public static final Double DEFAULT_GRACEFUL_SHUTDOWN_WAIT_LOOP_S = 2.0;

  public static final Double DEFAULT_HEALTH_CHECK_PERIOD_S = 10.0;

  public static final Double DEFAULT_HEALTH_CHECK_TIMEOUT_S = 30.0;
}
