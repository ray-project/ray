package org.ray.api.test;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.RayRemote;
import org.ray.util.FileUtil;

/**
 * given a directory of document files on each "machine", we would like to count the appearance of
 * some word.
 */
public class WordCountTest {

  @RayRemote
  public static List<String> getMachineList() {
    return Arrays.asList("A", "B", "C");
  }

  @RayRemote
  public static Integer countWord(String machine, String word) {

    String log;
    try {
      log = FileUtil.readResourceFile("mapreduce/" + machine + ".log");
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      log = "";
    }
    log = log.toLowerCase();
    int start = 0;
    int count = 0;
    while (true) {
      if (start >= log.length()) {
        break;
      }
      int index = log.indexOf(word, start);
      if (index == -1) {
        break;
      }
      start = index + word.length();
      count++;
    }
    return count;
  }

  @RayRemote
  public static Integer sum(Integer a, Integer/*TODO modify int to Integer in ASM hook*/ b) {
    return a + b;
  }

  //@Test
  public void test() {
    int sum = mapReduce();
    Assert.assertEquals(sum, 143);
  }

  public int mapReduce() {
    RayObject<List<String>> machines = Ray.call(WordCountTest::getMachineList);
    RayObject<Integer> total = null;
    for (String machine : machines.get()) {
      RayObject<Integer> wordcount = Ray.call(WordCountTest::countWord, machine, "ray");
      if (total == null) {
        total = wordcount;
      } else {
        total = Ray.call(WordCountTest::sum, total, wordcount);
      }
    }
    return total.get();
  }
}
