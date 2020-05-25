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

package io.ray.streaming.state.serialization.impl;

import com.google.common.hash.Hashing;
import io.ray.streaming.state.StateException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AbstractSerialization. Generate row key.
 */
public abstract class AbstractSerialization {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSerialization.class);

  public String generateRowKeyPrefix(String key) {
    if (StringUtils.isNotEmpty(key)) {
      String md5 = Hashing.md5().hashUnencodedChars(key).toString();
      if ("".equals(md5)) {
        throw new StateException("Invalid value to md5:" + key);
      }
      return StringUtils.substring(md5, 0, 4) + ":" + key;
    } else {
      LOG.warn("key is empty");
      return key;
    }
  }
}
