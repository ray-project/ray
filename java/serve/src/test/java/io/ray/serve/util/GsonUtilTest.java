package io.ray.serve.util;

import com.google.common.collect.ImmutableMap;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Map;

/** GsonUtil Tester. */
public class GsonUtilTest {
  @Test
  public void testDisableEscapeHtmlChars() throws Exception {
    String html = "<font color=\"#FFFFFF\">";
    String json = GsonUtil.toJson(ImmutableMap.of("html", html));
    Map map = GsonUtil.fromJson(json, Map.class);
    Assert.assertEquals(getString(map, "html"), html);
    System.out.println(map);
  }

  public static String getString(Map map, Object key) {
    if (map != null) {
      Object answer = map.get(key);
      if (answer != null) {
        return answer.toString();
      }
    }
    return null;
  }
}
