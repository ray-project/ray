package org.ray.api.experiment.mr;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.commons.lang3.tuple.Pair;
import org.ray.api.Ray;
import org.ray.api.RayList;
import org.ray.api.RayObject;
import org.ray.api.RayRemote;

/**
 * mimic the MapReduce interface atop of Ray API (in memory version)
 */
public class MemoryMapReduce<TInput, TMapKey, TMapValue, TReduceValue> {

  public List<Pair<TMapKey, TMapValue>> Map(TInput input) throws Exception {
    throw new Exception("not implemented");
  }

  public TReduceValue Reduce(TMapKey k, List<TMapValue> values) throws Exception {
    throw new Exception("not implemented");
  }

  //
  // main logic to execute this map-reduce with remote mappers and reducers
  //
  // @param inputs - given input file segments each containing List<TInput>
  // @return output file segments each containing SortedMap<TMapKey, TReduceValue>
  //
  public SortedMap<TMapKey, TReduceValue> Run(List<List<TInput>> inputs, int mapperCount,
      Integer reducerCount) {
    // start all mappers
    ArrayList<RayList<SortedMap<TMapKey, List<TMapValue>>>> mappers = new ArrayList<>();

    int inputCountPerMap = inputs.size() / mapperCount;
    int index = 0;
    for (int i = 0; i < mapperCount; i++) {
      if (index >= inputs.size()) {
        break;
      }

      List<List<TInput>> perMapInputs = new ArrayList<>();
      for (int j = 0; j < inputCountPerMap && index < inputs.size(); j++) {
        perMapInputs.add(inputs.get(index++));
      }

      mappers.add(Ray.call_n(MemoryMapReduce::InternalMap, reducerCount, reducerCount, perMapInputs,
          this.getClass().getName()));
    }

    // BSP barrier for all mappers to be finished
    // this is unnecessary as later on we call map.get() to wait for mappers to be completed
    // Ray.wait(mappers.toArray(new RayObject<?>[0]), mappers.size(), 0);

    // start all reducers
    ArrayList<RayObject<SortedMap<TMapKey, TReduceValue>>> reducers = new ArrayList<>();
    for (int i = 0; i < reducerCount; i++) {
      // collect states from mappers for this reducer
      RayList<SortedMap<TMapKey, List<TMapValue>>> fromMappers = new RayList();
      for (int j = 0; j < mapperCount; j++) {
        assert (mappers.get(j).size() == reducerCount);
        fromMappers.add(mappers.get(j).Get(i));
      }

      // start this reducer with given input
      reducers.add(Ray.call(MemoryMapReduce::InternalReduce,
          (List<SortedMap<TMapKey, List<TMapValue>>>) fromMappers, this.getClass().getName()));
    }

    // BSP barrier for all reducers to be finished
    // this is unnecessary coz we will call reducer.get() to wait for their completion
    // Ray.wait(reducers.toArray(new RayObject<?>[0]), reducers.size(), 0);

    // collect outputs
    TreeMap<TMapKey, TReduceValue> outputs = new TreeMap<>();
    for (RayObject<SortedMap<TMapKey, TReduceValue>> r : reducers) {
      r.get().forEach(outputs::put);
    }
    return outputs;
  }

  //
  // given a set of input files, output another set of files for reducers
  //
  // @param inputs - for each input file, it contains List<TInput>
  // @return %recuderCount% number of files for the reducers numbering from 0 to reducerCount - 1.
  //         for each output file, it contains SortedMap<TMapKey, List<TMapValue>>
  //
  @RayRemote
  public static <TInput, TMapKey, TMapValue, TReduceValue> List<SortedMap<TMapKey, List<TMapValue>>> InternalMap(
      Integer reducerCount,
      List<List<TInput>> inputs,
      String mrClassName) {
    MemoryMapReduce<TInput, TMapKey, TMapValue, TReduceValue> mr;
    try {
      mr = (MemoryMapReduce<TInput, TMapKey, TMapValue, TReduceValue>) Class
          .forName(mrClassName).getConstructors()[0].newInstance();
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException | SecurityException | ClassNotFoundException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
      return null;
    }

    ArrayList<SortedMap<TMapKey, List<TMapValue>>> out = new ArrayList<>();
    for (int i = 0; i < reducerCount; i++) {
      out.add(new TreeMap<>());
    }

    for (List<TInput> inputSeg : inputs) {
      for (TInput input : inputSeg) {
        try {
          List<Pair<TMapKey, TMapValue>> result = mr.Map(input);
          for (Pair<TMapKey, TMapValue> pr : result) {
            int reducerIndex = Math.abs(pr.getKey().hashCode()) % reducerCount;
            out.get(reducerIndex).computeIfAbsent(pr.getKey(), k -> new ArrayList<>());
            out.get(reducerIndex).get(pr.getKey()).add(pr.getValue());
          }
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }

    return out;
  }

  //public static class Helper {
  //
  // given a set of input sets from all mappers, performance merge sort and apply reducer function
  //
  // @param inputs each file contains SortedMap<TMapKey, List<TMapValue>>
  // @return an output file contains SortedMap<TMapKey, TReduceValue>
  //
  @RayRemote
  public static <TInput, TMapKey, TMapValue, TReduceValue> SortedMap<TMapKey, TReduceValue> InternalReduce(
      List<SortedMap<TMapKey, List<TMapValue>>> inputs,
      String mrClassName) {
    MemoryMapReduce<TInput, TMapKey, TMapValue, TReduceValue> mr;
    try {
      mr = (MemoryMapReduce<TInput, TMapKey, TMapValue, TReduceValue>) Class
          .forName(mrClassName).getConstructors()[0].newInstance();
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException | SecurityException | ClassNotFoundException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
      return null;
    }

    // merge inputs from many mappers
    TreeMap<TMapKey, List<TMapValue>> minputs = new TreeMap<>();
    for (SortedMap<TMapKey, List<TMapValue>> input : inputs) {
      for (Map.Entry<TMapKey, List<TMapValue>> entry : input.entrySet()) {
        if (!minputs.containsKey(entry.getKey())) {
          minputs.put(entry.getKey(), new ArrayList<>());
        }
        minputs.get(entry.getKey()).addAll(entry.getValue());
      }
    }

    // reduce
    TreeMap<TMapKey, TReduceValue> out = new TreeMap<>();
    for (Map.Entry<TMapKey, List<TMapValue>> entry : minputs.entrySet()) {
      try {
        out.put(entry.getKey(), mr.Reduce(entry.getKey(), entry.getValue()));
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    return out;
  }
  // }
}
