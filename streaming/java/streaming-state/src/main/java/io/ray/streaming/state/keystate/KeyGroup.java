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
import java.io.Serializable;

/**
 * This class defines key-groups. Key-groups is the key space in a job, which is partitioned for
 * keyed state processing in state backend. The boundaries of the key-group are inclusive.
 */
public class KeyGroup implements Serializable {

  private final int startIndex;
  private final int endIndex;

  /**
   * Defines the range [startIndex, endIndex]
   *
   * @param startIndex start of the range (inclusive)
   * @param endIndex end of the range (inclusive)
   */
  public KeyGroup(int startIndex, int endIndex) {
    Preconditions.checkArgument(startIndex >= 0 && startIndex <= endIndex);
    this.startIndex = startIndex;
    this.endIndex = endIndex;
    Preconditions.checkArgument(size() >= 0, "overflow detected.");
  }

  /** Returns The number of key-group in the range */
  public int size() {
    return 1 + endIndex - startIndex;
  }

  public int getStartIndex() {
    return startIndex;
  }

  public int getEndIndex() {
    return endIndex;
  }

  @Override
  public String toString() {
    return "KeyGroup{" + "startIndex=" + startIndex + ", endIndex=" + endIndex + '}';
  }
}
