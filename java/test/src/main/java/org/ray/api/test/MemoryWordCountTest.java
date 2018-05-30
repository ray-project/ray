package org.ray.api.test;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ray.api.experiment.mr.MemoryMapReduce;

/**
 * test the MapReduce interface
 */
@RunWith(MyRunner.class)
public class MemoryWordCountTest {

  public static class MemoryWordCount extends MemoryMapReduce<String, String, Integer, Integer> {

    public List<Pair<String, Integer>> Map(String input) {
      ArrayList<Pair<String, Integer>> counts = new ArrayList<>();
      for (String s : input.split(" ")) {
        counts.add(Pair.of(s, 1));
      }
      return counts;
    }

    public Integer Reduce(String k, List<Integer> values) {
      return values.size();
    }
  }

  @Test
  public void test() {
    List<List<String>> iinputs = new ArrayList<>();
    List<String> inputs = new ArrayList<>();
    inputs.add("1 3 5 7 9");
    inputs.add("0 2 4 6 8");
    iinputs.add(inputs);

    inputs = new ArrayList<>();
    inputs.add("1 2 3 4 5 6 7 8 9 0");
    inputs.add("1 3 5 7 9");
    iinputs.add(inputs);

    inputs = new ArrayList<>();
    inputs.add("1 2 3 4 5 6 7 8 9 0");
    inputs.add("0 2 4 6 8");
    iinputs.add(inputs);

    inputs = new ArrayList<>();
    inputs.add("1 2 3 4 5 6 7 8 9 0");
    inputs.add("1 3 5 7 9");
    inputs.add("0 2 4 6 8");
    iinputs.add(inputs);

    MemoryWordCount wc = new MemoryWordCount();
    SortedMap<String, Integer> result = wc.Run(iinputs, 2, 2);

    Assert.assertEquals(6, (int) result.get("0"));
    Assert.assertEquals(6, (int) result.get("1"));
    Assert.assertEquals(6, (int) result.get("2"));
    Assert.assertEquals(6, (int) result.get("3"));
    Assert.assertEquals(6, (int) result.get("4"));
    Assert.assertEquals(6, (int) result.get("5"));
    Assert.assertEquals(6, (int) result.get("6"));
    Assert.assertEquals(6, (int) result.get("7"));
    Assert.assertEquals(6, (int) result.get("8"));
    Assert.assertEquals(6, (int) result.get("9"));
  }
}
