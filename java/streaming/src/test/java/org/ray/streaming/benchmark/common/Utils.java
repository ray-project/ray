/**
 * Copyright 2015, Yahoo Inc. Licensed under the terms of the Apache License 2.0. Please see LICENSE
 * file in the project root for terms.
 */
package org.ray.streaming.benchmark.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

public class Utils {

  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

  public static Map findAndReadConfigFile(String name, boolean mustExist) {
    InputStream in = null;
    boolean confFileEmpty = false;
    try {
      in = getConfigFileInputStream(name);
      if (null != in) {
        Yaml yaml = new Yaml(new SafeConstructor());
        Map ret = (Map) yaml.load(new InputStreamReader(in));
        if (null != ret) {
          return new HashMap(ret);
        } else {
          confFileEmpty = true;
        }
      }

      if (mustExist) {
        if (confFileEmpty) {
          throw new RuntimeException(
              "Config file " + name + " doesn't have any valid storm configs");
        } else {
          throw new RuntimeException("Could not find config file on classpath " + name);
        }
      } else {
        return new HashMap();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (null != in) {
        try {
          in.close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private static InputStream getConfigFileInputStream(String configFilePath)
      throws IOException {
    if (null == configFilePath) {
      throw new IOException(
          "Could not find config file, name not specified");
    }

    HashSet<URL> resources = new HashSet<URL>(findResources(configFilePath));
    if (resources.isEmpty()) {
      File configFile = new File(configFilePath);
      if (configFile.exists()) {
        return new FileInputStream(configFile);
      }
    } else if (resources.size() > 1) {
      throw new IOException(
          "Found multiple " + configFilePath
              + " resources. You're probably bundling the Storm jars with your topology jar. "
              + resources);
    } else {
      LOG.debug("Using " + configFilePath + " from resources");
      URL resource = resources.iterator().next();
      return resource.openStream();
    }
    return null;
  }

  public static List<URL> findResources(String name) {
    try {
      Enumeration<URL> resources = Thread.currentThread().getContextClassLoader()
          .getResources(name);
      List<URL> ret = new ArrayList<URL>();
      while (resources.hasMoreElements()) {
        ret.add(resources.nextElement());
      }
      return ret;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) {
    Map conf = Utils.findAndReadConfigFile("conf/benchmarkConf.yaml", true);
    System.out.println(conf);
  }
}
