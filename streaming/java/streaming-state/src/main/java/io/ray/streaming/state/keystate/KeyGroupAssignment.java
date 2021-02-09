/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.ray.streaming.state.keystate;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** This class defines key-group assignment algorithm。 */
public final class KeyGroupAssignment {

  /**
   * Computes the range of key-groups that are assigned for a given operator instance.
   *
   * @param maxParallelism Maximal parallelism of the job.
   * @param parallelism Parallelism for the job. <= maxParallelism.
   * @param index index of the operator instance.
   */
  public static KeyGroup getKeyGroup(int maxParallelism, int parallelism, int index) {
    Preconditions.checkArgument(
        maxParallelism >= parallelism,
        "Maximum parallelism (%s) must not be smaller than parallelism(%s)",
        maxParallelism,
        parallelism);

    int start = index == 0 ? 0 : ((index * maxParallelism - 1) / parallelism) + 1;
    int end = ((index + 1) * maxParallelism - 1) / parallelism;
    return new KeyGroup(start, end);
  }

  /**
   * Assigning the key to a key-group index.
   *
   * @param key the key to assign.
   * @param maxParallelism the maximum parallelism. Returns the key-group index to which the given
   *     key is assigned.
   */
  public static int assignKeyGroupIndexForKey(Object key, int maxParallelism) {
    return Math.abs(key.hashCode() % maxParallelism);
  }

  public static Map<Integer, List<Integer>> computeKeyGroupToTask(
      int maxParallelism, List<Integer> targetTasks) {
    Map<Integer, List<Integer>> keyGroupToTask = new ConcurrentHashMap<>();
    for (int index = 0; index < targetTasks.size(); index++) {
      KeyGroup taskKeyGroup = getKeyGroup(maxParallelism, targetTasks.size(), index);
      for (int groupId = taskKeyGroup.getStartIndex();
          groupId <= taskKeyGroup.getEndIndex();
          groupId++) {
        keyGroupToTask.put(groupId, ImmutableList.of(targetTasks.get(index)));
      }
    }
    return keyGroupToTask;
  }
}
