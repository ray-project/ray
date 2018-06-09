package org.ray.util.config;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Vector;
import org.ini4j.Config;
import org.ini4j.Ini;
import org.ini4j.Profile;
import org.ray.util.ObjectUtil;
import org.ray.util.StringUtil;

/**
 * Loads configurations from a file.
 */
public class ConfigReader {

  private final CurrentUseConfig currentUseConfig = new CurrentUseConfig();

  private final Ini ini = new Ini();

  private String file = "";

  public ConfigReader(String filePath) throws Exception {
    this(filePath, null);
  }

  public ConfigReader(String filePath, String updateConfigStr) throws Exception {
    System.out.println("Build ConfigReader, the file path " + filePath + " ,the update config str "
        + updateConfigStr);
    try {
      loadConfigFile(filePath);
      updateConfigFile(updateConfigStr);
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }

  }

  private void loadConfigFile(String filePath) throws Exception {

    this.currentUseConfig.filePath = filePath;
    String configFileDir = (new File(filePath)).getAbsoluteFile().getParent();
    byte[] encoded = Files.readAllBytes(Paths.get(filePath));
    String content = new String(encoded, StandardCharsets.UTF_8);
    content = content.replaceAll("%CONFIG_FILE_DIR%", configFileDir);

    InputStream fis = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    Config config = new Config();
    ini.setConfig(config);
    ini.load(fis);
    file = currentUseConfig.filePath;
  }

  private void updateConfigFile(String updateConfigStr) {

    if (updateConfigStr == null) {
      return;
    }
    String[] updateConfigArray = updateConfigStr.split(";");
    for (String currentUpdateConfig : updateConfigArray) {
      if (StringUtil.isNullOrEmpty(currentUpdateConfig)) {
        continue;
      }

      String[] currentUpdateConfigArray = currentUpdateConfig.split("=");
      String sectionAndItemKey;
      String value = "";
      if (currentUpdateConfigArray.length == 2) {
        sectionAndItemKey = currentUpdateConfigArray[0];
        value = currentUpdateConfigArray[1];
      } else if (currentUpdateConfigArray.length == 1) {
        sectionAndItemKey = currentUpdateConfigArray[0];
      } else {
        String errorMsg = "invalid config (must be of k=v or k or k=): " + currentUpdateConfig;
        System.err.println(errorMsg);
        throw new RuntimeException(errorMsg);
      }

      int splitOffset = sectionAndItemKey.lastIndexOf(".");
      int len = sectionAndItemKey.length();
      if (splitOffset < 1 || splitOffset == len - 1) {
        String errorMsg =
            "invalid config (no '.' found for section name and key):" + currentUpdateConfig;
        System.err.println(errorMsg);
        throw new RuntimeException(errorMsg);
      }

      String sectionKey = sectionAndItemKey.substring(0, splitOffset);
      String itemKey = sectionAndItemKey.substring(splitOffset + 1);
      if (ini.containsKey(sectionKey)) {
        ini.get(sectionKey).put(itemKey, value);
      } else {
        ini.add(sectionKey, itemKey, value);
      }
    }
  }

  public String filePath() {
    return file;
  }

  public CurrentUseConfig getCurrentUseConfig() {
    return currentUseConfig;
  }

  public String getStringValue(String sectionKey, String configKey, String defaultValue,
                               String dsptr) {
    String value = getOriValue(sectionKey, configKey, defaultValue, dsptr);
    if (value != null) {
      return value;
    } else {
      return defaultValue;
    }
  }

  public boolean getBooleanValue(String sectionKey, String configKey, boolean defaultValue,
                                 String dsptr) {
    String value = getOriValue(sectionKey, configKey, defaultValue, dsptr);
    if (value != null) {
      if (value.length() == 0) {
        return defaultValue;
      } else {
        return Boolean.valueOf(value);
      }
    } else {
      return defaultValue;
    }
  }

  public int getIntegerValue(String sectionKey, String configKey, int defaultValue, String dsptr) {
    String value = getOriValue(sectionKey, configKey, defaultValue, dsptr);
    if (value != null) {
      if (value.length() == 0) {
        return defaultValue;
      } else {
        return Integer.valueOf(value);
      }
    } else {
      return defaultValue;
    }
  }

  private synchronized <T> String getOriValue(String sectionKey, String configKey, T defaultValue,
                                              String deptr) {
    if (null == deptr) {
      throw new RuntimeException("desc must not be empty of the key:" + configKey);
    }
    Profile.Section section = ini.get(sectionKey);
    String oriValue = null;
    if (section != null && section.containsKey(configKey)) {
      oriValue = section.get(configKey);
    }

    if (!currentUseConfig.sectionMap.containsKey(sectionKey)) {
      ConfigSection configSection = new ConfigSection();
      configSection.sectionKey = sectionKey;
      updateConfigSection(configSection, configKey, defaultValue, deptr, oriValue);
      currentUseConfig.sectionMap.put(sectionKey, configSection);
    } else if (!currentUseConfig.sectionMap.get(sectionKey).itemMap.containsKey(configKey)) {
      ConfigSection configSection = currentUseConfig.sectionMap.get(sectionKey);
      updateConfigSection(configSection, configKey, defaultValue, deptr, oriValue);
    }
    return oriValue;
  }

  private <T> void updateConfigSection(ConfigSection configSection, String configKey,
                                       T defaultValue, String deptr, String oriValue) {
    ConfigItem<T> configItem = new ConfigItem<>();
    configItem.defaultValue = defaultValue;
    configItem.key = configKey;
    configItem.oriValue = oriValue;
    configItem.desc = deptr;
    configSection.itemMap.put(configKey, configItem);
  }

  public long getLongValue(String sectionKey, String configKey, long defaultValue, String dsptr) {
    String value = getOriValue(sectionKey, configKey, defaultValue, dsptr);
    if (value != null) {
      if (value.length() == 0) {
        return defaultValue;
      } else {
        return Long.valueOf(value);
      }
    } else {
      return defaultValue;
    }
  }

  public double getDoubleValue(String sectionKey, String configKey, double defaultValue,
                               String dsptr) {
    String value = getOriValue(sectionKey, configKey, defaultValue, dsptr);
    if (value != null) {
      if (value.length() == 0) {
        return defaultValue;
      } else {
        return Double.valueOf(value);
      }
    } else {
      return defaultValue;
    }
  }


  public int[] getIntegerArray(String sectionKey, String configKey, int[] defaultValue,
                               String dsptr) {
    String value = getOriValue(sectionKey, configKey, defaultValue, dsptr);
    int[] array = defaultValue;
    if (value != null) {
      String[] list = value.split(",");
      array = new int[list.length];
      for (int i = 0; i < list.length; i++) {
        array[i] = Integer.valueOf(list[i]);
      }
    }
    return array;
  }

  /**
   * get a string list from a whole section as keys e.g., [core] data_dirs = local.dirs # or
   * cluster.dirs
   * [local.dirs] /home/xxx/1 /home/yyy/2
   * [cluster.dirs] ...
   *
   * @param sectionKey          e.g., core
   * @param configKey           e.g., data_dirs
   * @param indirectSectionName e.g., cluster.dirs
   * @return string list
   */
  public String[] getIndirectStringArray(String sectionKey, String configKey,
                                         String indirectSectionName, String dsptr) {
    String s = getStringValue(sectionKey, configKey, indirectSectionName, dsptr);
    Profile.Section section = ini.get(s);
    if (section == null) {
      return new String[] {};
    } else {
      return section.keySet().toArray(new String[] {});
    }
  }

  public <T> void readObject(String sectionKey, T obj, T defaultValues) {
    for (Field fld : obj.getClass().getFields()) {
      Object defaultFldValue;
      try {
        defaultFldValue = defaultValues != null ? fld.get(defaultValues) : null;
      } catch (IllegalArgumentException | IllegalAccessException e) {
        defaultFldValue = null;
      }

      String section = sectionKey;
      String comment;
      String splitters = ", \t";
      String defaultArrayIndirectSectionName;
      AConfig[] anns = fld.getAnnotationsByType(AConfig.class);
      if (anns.length > 0) {
        comment = anns[0].comment();
        if (!StringUtil.isNullOrEmpty(anns[0].splitters())) {
          splitters = anns[0].splitters();
        }
        defaultArrayIndirectSectionName = anns[0].defaultArrayIndirectSectionName();

        // redirect the section if necessary
        if (!StringUtil.isNullOrEmpty(anns[0].defaultIndirectSectionName())) {
          section = this
              .getStringValue(sectionKey, fld.getName(), anns[0].defaultIndirectSectionName(),
                  comment);
        }
      } else {
        throw new RuntimeException("unspecified comment, please use @AConfig(comment = xxxx) for "
            + obj.getClass().getName() + "." + fld.getName() + "'s configuration descriptions ");
      }

      try {
        if (fld.getType().isPrimitive()) {
          if (fld.getType().equals(boolean.class)) {
            boolean v = getBooleanValue(section, fld.getName(), (boolean) defaultFldValue, comment);
            fld.set(obj, v);
          } else if (fld.getType().equals(float.class)) {
            float v = (float) getDoubleValue(section, fld.getName(),
                (double) (float) defaultFldValue, comment);
            fld.set(obj, v);
          } else if (fld.getType().equals(double.class)) {
            double v = getDoubleValue(section, fld.getName(), (double) defaultFldValue, comment);
            fld.set(obj, v);
          } else if (fld.getType().equals(byte.class)) {
            byte v = (byte) getLongValue(section, fld.getName(), (long) (byte) defaultFldValue,
                comment);
            fld.set(obj, v);
          } else if (fld.getType().equals(char.class)) {
            char v = (char) getLongValue(section, fld.getName(), (long) (char) defaultFldValue,
                comment);
            fld.set(obj, v);
          } else if (fld.getType().equals(short.class)) {
            short v = (short) getLongValue(section, fld.getName(), (long) (short) defaultFldValue,
                comment);
            fld.set(obj, v);
          } else if (fld.getType().equals(int.class)) {
            int v = (int) getLongValue(section, fld.getName(), (long) (int) defaultFldValue,
                comment);
            fld.set(obj, v);
          } else if (fld.getType().equals(long.class)) {
            long v = getLongValue(section, fld.getName(), (long) defaultFldValue, comment);
            fld.set(obj, v);
          } else {
            throw new RuntimeException("unhandled type " + fld.getType().getName());
          }
        } else if (fld.getType().equals(String.class)) {
          String v = getStringValue(section, fld.getName(), (String) defaultFldValue, comment);
          fld.set(obj, v);
        } else if (fld.getType().isEnum()) {
          String sv = getStringValue(section, fld.getName(), defaultFldValue.toString(), comment);
          @SuppressWarnings({"unchecked", "rawtypes"})
          Object v = Enum.valueOf((Class<Enum>) fld.getType(), sv);
          fld.set(obj, v);
          // TODO: this is a hack and needs to be resolved later
        } else if (fld.getType().getName().equals("org.ray.api.UniqueID")) {
          String sv = getStringValue(section, fld.getName(), defaultFldValue.toString(), comment);
          Object v;
          try {
            v = fld.getType().getConstructor(new Class<?>[] {String.class}).newInstance(sv);
          } catch (NoSuchMethodException | SecurityException | InstantiationException
              | InvocationTargetException e) {
            System.err.println(
                section + "." + fld.getName() + "'s format (" + sv + ") is invalid, default to "
                    + defaultFldValue.toString());
            v = defaultFldValue;
          }
          fld.set(obj, v);
        } else if (fld.getType().isArray()) {
          Class<?> ccls = fld.getType().getComponentType();
          String ss = getStringValue(section, fld.getName(), null, comment);
          if (null == ss) {
            fld.set(obj, defaultFldValue);
          } else {
            Vector<String> ls = StringUtil.split(ss, splitters, "", "");
            if (ccls.equals(boolean.class)) {
              boolean[] v = ObjectUtil
                  .toBooleanArray(ls.stream().map(Boolean::parseBoolean).toArray());
              fld.set(obj, v);
            } else if (ccls.equals(double.class)) {
              double[] v = ls.stream().mapToDouble(Double::parseDouble).toArray();
              fld.set(obj, v);
            } else if (ccls.equals(int.class)) {
              int[] v = ls.stream().mapToInt(Integer::parseInt).toArray();
              fld.set(obj, v);
            } else if (ccls.equals(long.class)) {
              long[] v = ls.stream().mapToLong(Long::parseLong).toArray();
              fld.set(obj, v);
            } else if (ccls.equals(String.class)) {
              String[] v;
              if (StringUtil.isNullOrEmpty(defaultArrayIndirectSectionName)) {
                v = ls.toArray(new String[] {});
              } else {
                v = this
                    .getIndirectStringArray(section, fld.getName(),
                        defaultArrayIndirectSectionName,
                        comment);
              }
              fld.set(obj, v);
            } else {
              throw new RuntimeException(
                  "Array with component type " + ccls.getName() + " is not supported yet");
            }
          }
        } else {
          Object fldObj = ObjectUtil.newObject(fld.getType());
          fld.set(obj, fldObj);
          readObject(section + "." + fld.getName(), fldObj, defaultFldValue);
        }
      } catch (IllegalArgumentException | IllegalAccessException e) {
        throw new RuntimeException("set fld " + fld.getName() + " failed, err = " + e.getMessage(),
            e);
      }
    }
  }

}
