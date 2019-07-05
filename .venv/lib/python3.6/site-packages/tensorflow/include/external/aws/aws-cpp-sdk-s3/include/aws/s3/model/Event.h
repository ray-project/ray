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

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSString.h>

namespace Aws
{
namespace S3
{
namespace Model
{
  enum class Event
  {
    NOT_SET,
    s3_ReducedRedundancyLostObject,
    s3_ObjectCreated,
    s3_ObjectCreated_Put,
    s3_ObjectCreated_Post,
    s3_ObjectCreated_Copy,
    s3_ObjectCreated_CompleteMultipartUpload,
    s3_ObjectRemoved,
    s3_ObjectRemoved_Delete,
    s3_ObjectRemoved_DeleteMarkerCreated
  };

namespace EventMapper
{
AWS_S3_API Event GetEventForName(const Aws::String& name);

AWS_S3_API Aws::String GetNameForEvent(Event value);
} // namespace EventMapper
} // namespace Model
} // namespace S3
} // namespace Aws
