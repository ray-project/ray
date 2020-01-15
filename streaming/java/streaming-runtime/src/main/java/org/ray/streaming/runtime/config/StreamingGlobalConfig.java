package org.ray.streaming.runtime.config;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.aeonbits.owner.Config.DefaultValue;
import org.aeonbits.owner.Config.Key;
import org.aeonbits.owner.ConfigFactory;
import org.ray.streaming.runtime.config.global.CommonConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming general config. May used by both JobMaster and JobWorker.
 */
public class StreamingGlobalConfig implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(StreamingGlobalConfig.class);

  public final CommonConfig commonConfig;

  public final Map<String, String> configMap;

  public StreamingGlobalConfig(final Map<String, String> conf) {
    configMap = new HashMap<>(conf);

    commonConfig = ConfigFactory.create(CommonConfig.class, conf);
    globalConfig2Map();
  }

  @Override
  public String toString() {
    return configMap.toString();
  }

  private void globalConfig2Map() {
    try {
      configMap.putAll(config2Map(this.commonConfig));
    } catch (Exception e) {
      LOG.error("Couldn't convert global config to a map.", e);
    }
  }

  protected Map<String, String> config2Map(org.aeonbits.owner.Config config)
      throws ClassNotFoundException {
    Map<String, String> result = new HashMap<>();

    Class<?> proxyClazz = Class.forName(config.getClass().getName());
    Class<?>[] proxyInterfaces = proxyClazz.getInterfaces();

    Class<?> configInterface = null;
    for (Class<?> proxyInterface : proxyInterfaces) {
      if (Config.class.isAssignableFrom(proxyInterface)) {
        configInterface = proxyInterface;
        break;
      }
    }
    Preconditions.checkArgument(configInterface != null,
        "Can not get config interface.");
    Method[] methods = configInterface.getMethods();

    for (Method method : methods) {
      Key ownerKeyAnnotation = method.getAnnotation(Key.class);
      String ownerKeyAnnotationValue;
      if (ownerKeyAnnotation != null) {
        ownerKeyAnnotationValue = ownerKeyAnnotation.value();
        Object value;
        try {
          value = method.invoke(config);
        } catch (Exception e) {
          LOG.warn("Can not get value by method invoking for config key: {}. "
              + "So use default value instead.", ownerKeyAnnotationValue);
          String defaultValue = method.getAnnotation(DefaultValue.class).value();
          value = defaultValue;
        }
        result.put(ownerKeyAnnotationValue, value + "");
      }
    }
    return result;
  }
}
