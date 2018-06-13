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
 * mimic the MapReduce interface atop of Ray API (in memory version).
 */
public class MemoryMapReduce<TInputT, TMapKeyT, TMapValueT, TReduceValueT> {

  public List<Pair<TMapKeyT, TMapValueT>> map(TInputT input) throws Exception {
    throw new Exception("not implemented");
  }

  public TReduceValueT reduce(TMapKeyT k, List<TMapValueT> values) throws Exception {
    throw new Exception("not implemented");
  }

  //
  // main logic to execute this map-reduce with remote mappers and reducers
  //
  // @param inputs - given input file segments each containing List<TInput>
  // @return output file segments each containing SortedMap<TMapKey, TReduceValue>
  //
  public SortedMap<TMapKeyT, TReduceValueT> run(List<List<TInputT>> inputs, int mapperCount,
                                                Integer reducerCount) {
    // start all mappers
    ArrayList<RayList<SortedMap<TMapKeyT, List<TMapValueT>>>> mappers = new ArrayList<>();

    int inputCountPerMap = inputs.size() / mapperCount;
    int index = 0;
    for (int i = 0; i < mapperCount; i++) {
      if (index >= inputs.size()) {
        break;
      }

      List<List<TInputT>> perMapInputs = new ArrayList<>();
      for (int j = 0; j < inputCountPerMap && index < inputs.size(); j++) {
        perMapInputs.add(inputs.get(index++));
      }

      mappers.add(Ray.call_n(MemoryMapReduce::internalMap, reducerCount, reducerCount, perMapInputs,
          this.getClass().getName()));
    }

    // BSP barrier for all mappers to be finished
    // this is unnecessary as later on we call map.get() to wait for mappers to be completed
    // Ray.wait(mappers.toArray(new RayObject<?>[0]), mappers.size(), 0);

    // start all reducers
    ArrayList<RayObject<SortedMap<TMapKeyT, TReduceValueT>>> reducers = new ArrayList<>();
    for (int i = 0; i < reducerCount; i++) {
      // collect states from mappers for this reducer
      RayList<SortedMap<TMapKeyT, List<TMapValueT>>> fromMappers = new RayList();
      for (int j = 0; j < mapperCount; j++) {
        assert (mappers.get(j).size() == reducerCount);
        fromMappers.add(mappers.get(j).Get(i));
      }

      // start this reducer with given input
      reducers.add(Ray.call(MemoryMapReduce::internalReduce,
          (List<SortedMap<TMapKeyT, List<TMapValueT>>>) fromMappers, this.getClass().getName()));
    }

    // BSP barrier for all reducers to be finished
    // this is unnecessary coz we will call reducer.get() to wait for their completion
    // Ray.wait(reducers.toArray(new RayObject<?>[0]), reducers.size(), 0);

    // collect outputs
    TreeMap<TMapKeyT, TReduceValueT> outputs = new TreeMap<>();
    for (RayObject<SortedMap<TMapKeyT, TReduceValueT>> r : reducers) {
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
  public static <TInputT, TMapKeyT, TMapValueT, TReduceValueT> List<SortedMap<TMapKeyT,
      List<TMapValueT>>> internalMap(
      Integer reducerCount,
      List<List<TInputT>> inputs,
      String mrClassName) {
    MemoryMapReduce<TInputT, TMapKeyT, TMapValueT, TReduceValueT> mr;
    try {
      mr = (MemoryMapReduce<TInputT, TMapKeyT, TMapValueT, TReduceValueT>) Class
          .forName(mrClassName).getConstructors()[0].newInstance();
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException | SecurityException | ClassNotFoundException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
      return null;
    }

    ArrayList<SortedMap<TMapKeyT, List<TMapValueT>>> out = new ArrayList<>();
    for (int i = 0; i < reducerCount; i++) {
      out.add(new TreeMap<>());
    }

    for (List<TInputT> inputSeg : inputs) {
      for (TInputT input : inputSeg) {
        try {
          List<Pair<TMapKeyT, TMapValueT>> result = mr.map(input);
          for (Pair<TMapKeyT, TMapValueT> pr : result) {
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
  public static <TInputT, TMapKeyT, TMapValueT, TReduceValueT> SortedMap<TMapKeyT, TReduceValueT>
      internalReduce(
      List<SortedMap<TMapKeyT, List<TMapValueT>>> inputs,
      String mrClassName) {
    MemoryMapReduce<TInputT, TMapKeyT, TMapValueT, TReduceValueT> mr;
    try {
      mr = (MemoryMapReduce<TInputT, TMapKeyT, TMapValueT, TReduceValueT>) Class
          .forName(mrClassName).getConstructors()[0].newInstance();
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException | SecurityException | ClassNotFoundException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
      return null;
    }

    // merge inputs from many mappers
    TreeMap<TMapKeyT, List<TMapValueT>> minputs = new TreeMap<>();
    for (SortedMap<TMapKeyT, List<TMapValueT>> input : inputs) {
      for (Map.Entry<TMapKeyT, List<TMapValueT>> entry : input.entrySet()) {
        if (!minputs.containsKey(entry.getKey())) {
          minputs.put(entry.getKey(), new ArrayList<>());
        }
        minputs.get(entry.getKey()).addAll(entry.getValue());
      }
    }

    // reduce
    TreeMap<TMapKeyT, TReduceValueT> out = new TreeMap<>();
    for (Map.Entry<TMapKeyT, List<TMapValueT>> entry : minputs.entrySet()) {
      try {
        out.put(entry.getKey(), mr.reduce(entry.getKey(), entry.getValue()));
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    return out;
  }
  // }
}
