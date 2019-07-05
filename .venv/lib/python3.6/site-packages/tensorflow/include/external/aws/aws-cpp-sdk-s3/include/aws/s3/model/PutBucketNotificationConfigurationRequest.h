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
#include <aws/s3/S3Request.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/s3/model/NotificationConfiguration.h>
#include <utility>

namespace Aws
{
namespace S3
{
namespace Model
{

  /**
   */
  class AWS_S3_API PutBucketNotificationConfigurationRequest : public S3Request
  {
  public:
    PutBucketNotificationConfigurationRequest();
    
    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "PutBucketNotificationConfiguration"; }

    Aws::String SerializePayload() const override;


    
    inline const Aws::String& GetBucket() const{ return m_bucket; }

    
    inline void SetBucket(const Aws::String& value) { m_bucketHasBeenSet = true; m_bucket = value; }

    
    inline void SetBucket(Aws::String&& value) { m_bucketHasBeenSet = true; m_bucket = std::move(value); }

    
    inline void SetBucket(const char* value) { m_bucketHasBeenSet = true; m_bucket.assign(value); }

    
    inline PutBucketNotificationConfigurationRequest& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    
    inline PutBucketNotificationConfigurationRequest& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    
    inline PutBucketNotificationConfigurationRequest& WithBucket(const char* value) { SetBucket(value); return *this;}


    
    inline const NotificationConfiguration& GetNotificationConfiguration() const{ return m_notificationConfiguration; }

    
    inline void SetNotificationConfiguration(const NotificationConfiguration& value) { m_notificationConfigurationHasBeenSet = true; m_notificationConfiguration = value; }

    
    inline void SetNotificationConfiguration(NotificationConfiguration&& value) { m_notificationConfigurationHasBeenSet = true; m_notificationConfiguration = std::move(value); }

    
    inline PutBucketNotificationConfigurationRequest& WithNotificationConfiguration(const NotificationConfiguration& value) { SetNotificationConfiguration(value); return *this;}

    
    inline PutBucketNotificationConfigurationRequest& WithNotificationConfiguration(NotificationConfiguration&& value) { SetNotificationConfiguration(std::move(value)); return *this;}

  private:

    Aws::String m_bucket;
    bool m_bucketHasBeenSet;

    NotificationConfiguration m_notificationConfiguration;
    bool m_notificationConfigurationHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
