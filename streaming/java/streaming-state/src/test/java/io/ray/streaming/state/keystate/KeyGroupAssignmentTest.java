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

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class KeyGroupAssignmentTest {


  @Test
  public void testComputeKeyGroupRangeForOperatorIndex() throws Exception {
    KeyGroup keyGroup = KeyGroupAssignment.getKeyGroup(4096, 1, 0);
    assertEquals(keyGroup.getStartIndex(), 0);
    assertEquals(keyGroup.getEndIndex(), 4095);
    assertEquals(keyGroup.size(), 4096);

    KeyGroup keyGroup2 = KeyGroupAssignment.getKeyGroup(4096, 2, 0);
    assertEquals(keyGroup2.getStartIndex(), 0);
    assertEquals(keyGroup2.getEndIndex(), 2047);
    assertEquals(keyGroup2.size(), 2048);

    keyGroup = KeyGroupAssignment.getKeyGroup(4096, 3, 0);
    assertEquals(keyGroup.getStartIndex(), 0);
    assertEquals(keyGroup.getEndIndex(), 1365);
    assertEquals(keyGroup.size(), 1366);

    keyGroup2 = KeyGroupAssignment.getKeyGroup(4096, 3, 1);
    assertEquals(keyGroup2.getStartIndex(), 1366);
    assertEquals(keyGroup2.getEndIndex(), 2730);
    assertEquals(keyGroup2.size(), 1365);

    KeyGroup keyGroup3 = KeyGroupAssignment.getKeyGroup(4096, 3, 2);
    assertEquals(keyGroup3.getStartIndex(), 2731);
    assertEquals(keyGroup3.getEndIndex(), 4095);
    assertEquals(keyGroup3.size(), 1365);
  }

}
