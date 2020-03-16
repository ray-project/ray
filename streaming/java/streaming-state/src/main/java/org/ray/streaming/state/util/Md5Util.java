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

package org.ray.streaming.state.util;

import java.nio.charset.StandardCharsets;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * Md5 utils for rowkey
 */
public class Md5Util {

  private Md5Util() {
  }

  public static String md5sum(byte[] b) {
    return DigestUtils.md5Hex(b);
  }


  public static String md5sum(String str) {
    if (StringUtils.isNotEmpty(str)) {
      byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
      return md5sum(bytes);
    }
    return "";
  }
}
