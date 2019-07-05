/*
  * Copyright 2010-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the "License").
  * You may not use this file except in compliance with the License.
  * A copy of the License is located at
  *
  *  http://aws.amazon.com/apache2.0
  *
  * or in the "license" file accompanying this file. This file is distributed
  * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
  * express or implied. See the License for the specific language governing
  * permissions and limitations under the License.
  */

#ifdef __ANDROID__

#include <aws/core/Core_EXPORTS.h>

#include <aws/core/utils/memory/stl/AWSString.h>

#include <jni.h>

namespace Aws
{
namespace Platform
{

// must be called before any other native SDK functions when running on Android
AWS_CORE_API void InitAndroid(JNIEnv* env, jobject context);

// helper functions for functionality requiring JNI calls; not valid until InitAndroid has been called
AWS_CORE_API JavaVM* GetJavaVM();
AWS_CORE_API const char* GetCacheDirectory();

} //namespace Platform
} //namespace Aws

#endif // __ANDROID__
