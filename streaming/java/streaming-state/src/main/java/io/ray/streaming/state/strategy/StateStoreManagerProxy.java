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

package io.ray.streaming.state.strategy;

import io.ray.streaming.state.StateStoreManager;
import io.ray.streaming.state.backend.AbstractKeyStateBackend;
import io.ray.streaming.state.backend.StateStrategy;
import io.ray.streaming.state.keystate.desc.AbstractStateDescriptor;
import io.ray.streaming.state.store.KeyValueStore;
import java.util.Map;

/**
 * This class support ITransactionState.
 *
 * <p>Based on the given StorageMode, different implementation instance of the AbstractStateStrategy
 * class will be created. All method calls will be delegated to the strategy instance.
 */
public abstract class StateStoreManagerProxy<V> implements StateStoreManager {

  protected final AbstractStateStoreManager<V> stateStrategy;
  private final AbstractKeyStateBackend keyStateBackend;

  public StateStoreManagerProxy(
      AbstractKeyStateBackend keyStateBackend, AbstractStateDescriptor stateDescriptor) {
    this.keyStateBackend = keyStateBackend;
    KeyValueStore<String, Map<Long, byte[]>> backStorage =
        keyStateBackend.getBackStorage(stateDescriptor);
    StateStrategy stateStrategy = keyStateBackend.getStateStrategy();
    switch (stateStrategy) {
      case DUAL_VERSION:
        this.stateStrategy = new DualStateStoreManager<>(backStorage);
        break;
      case SINGLE_VERSION:
        this.stateStrategy = new MVStateStoreManager<>(backStorage);
        break;
      default:
        throw new UnsupportedOperationException("store vertexType not support");
    }
  }

  protected void setKeyGroupIndex(int index) {
    this.stateStrategy.setKeyGroupIndex(index);
  }

  @Override
  public void finish(long checkpointId) {
    this.stateStrategy.finish(checkpointId);
  }

  /** The commit can be used in another thread to reach async state commit. */
  @Override
  public void commit(long checkpointId) {
    this.stateStrategy.commit(checkpointId);
  }

  /** The ackCommit must be called after commit in the same thread. */
  @Override
  public void ackCommit(long checkpointId, long timeStamp) {
    this.stateStrategy.ackCommit(checkpointId);
  }

  @Override
  public void rollBack(long checkpointId) {
    this.stateStrategy.rollBack(checkpointId);
  }

  public void close() {
    this.stateStrategy.close();
  }

  public V get(String key) {
    this.stateStrategy.setKeyGroupIndex(keyStateBackend.getKeyGroupIndex());
    return this.stateStrategy.get(this.keyStateBackend.getCheckpointId(), key);
  }

  public void put(String key, V value) {
    this.stateStrategy.setKeyGroupIndex(keyStateBackend.getKeyGroupIndex());
    this.stateStrategy.put(this.keyStateBackend.getCheckpointId(), key, value);
  }
}
