package org.ray.spi;

import org.ray.util.config.AConfig;
import org.ray.util.config.ConfigReader;

/**
 * Path related configurations.
 */
public class PathConfig {

  @AConfig(comment = "additional class path for JAVA",
      defaultArrayIndirectSectionName = "ray.java.path.classes.source")
  public String[] java_class_paths;

  @AConfig(comment = "additional JNI library paths for JAVA",
      defaultArrayIndirectSectionName = "ray.java.path.jni.build")
  public String[] java_jnilib_paths;

  @AConfig(comment = "path to ray_functions.txt for the default rewritten functions in ray runtime")
  public String java_runtime_rewritten_jars_dir = "";

  @AConfig(comment = "path to redis-server")
  public String redis_server;

  @AConfig(comment = "path to redis module")
  public String redis_module;

  @AConfig(comment = "path to plasma storage")
  public String store;

  @AConfig(comment = "path to plasma manager")
  public String store_manager;

  @AConfig(comment = "path to local scheduler")
  public String local_scheduler;

  @AConfig(comment = "path to global scheduler")
  public String global_scheduler;

  @AConfig(comment = "path to raylet")
  public String raylet;

  @AConfig(comment = "path to python directory")
  public String python_dir;

  @AConfig(comment = "path to log server")
  public String log_server;

  @AConfig(comment = "path to log server config file")
  public String log_server_config;

  public PathConfig(ConfigReader config) {
    if (config.getBooleanValue("ray.java.start", "deploy", false,
        "whether the package is used as a cluster deployment")) {
      config.readObject("ray.java.path.deploy", this, this);
    } else {
      boolean isJar = this.getClass().getResource(this.getClass().getSimpleName() + ".class")
          .getFile().split("!")[0].endsWith(".jar");
      if (isJar) {
        config.readObject("ray.java.path.package", this, this);
      } else {
        config.readObject("ray.java.path.source", this, this);
      }
    }
  }
}
