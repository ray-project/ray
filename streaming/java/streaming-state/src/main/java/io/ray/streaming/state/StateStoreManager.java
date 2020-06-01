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

package io.ray.streaming.state;

/**
 * TransactionState interface.
 * <p>
 * Streaming State should implement transaction in case of failure,
 * which in our case is four default method, finish, commit, ackCommit, rollback.
 */
public interface StateStoreManager {

  /**
   * The finish method is used when the batched data is all saved in state.
   * Normally, serialization job is done here.
   */
  void finish(long checkpointId);

  /**
   * The commit method is used for persistent, and can be used in another thread to reach async
   * state commit.
   * Normally, data persistent is done here.
   */
  void commit(long checkpointId);

  /**
   * The ackCommit method is used for cleaning the last checkpoint, and must be called after commit
   * in the same thread.
   */
  void ackCommit(long checkpointId, long timeStamp);

  /**
   * The rollback method is used for recovering the checkpoint.
   */
  void rollBack(long checkpointId);
}
