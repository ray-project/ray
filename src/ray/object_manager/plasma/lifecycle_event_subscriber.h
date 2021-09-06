// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "ray/common/id.h"

namespace plasma {

// ILifecycleEventSubscriber subscribes to plasma store state changes.
class ILifecycleEventSubscriber {
 public:
  virtual ~ILifecycleEventSubscriber() = default;

  // Called after a new object is created.
  virtual void OnObjectCreated(const ray::ObjectID &id) = 0;

  // Called after an object is sealed.
  virtual void OnObjectSealed(const ray::ObjectID &id) = 0;

  // Called BEFORE an object is deleted.
  virtual void OnObjectDeleting(const ray::ObjectID &id) = 0;

  // Called after an object's ref count is bumped by 1.
  virtual void OnObjectRefIncreased(const ray::ObjectID &id) = 0;

  // Called after an object's ref count is decreased by 1.
  virtual void OnObjectRefDecreased(const ray::ObjectID &id) = 0;
};
}  // namespace plasma