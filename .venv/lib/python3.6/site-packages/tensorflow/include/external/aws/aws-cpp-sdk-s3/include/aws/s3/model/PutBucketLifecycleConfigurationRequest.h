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
#include <aws/s3/model/BucketLifecycleConfiguration.h>
#include <utility>

namespace Aws
{
namespace S3
{
namespace Model
{

  /**
   */
  class AWS_S3_API PutBucketLifecycleConfigurationRequest : public S3Request
  {
  public:
    PutBucketLifecycleConfigurationRequest();
    
    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "PutBucketLifecycleConfiguration"; }

    Aws::String SerializePayload() const override;

    inline bool ShouldComputeContentMd5() const override { return true; }


    
    inline const Aws::String& GetBucket() const{ return m_bucket; }

    
    inline void SetBucket(const Aws::String& value) { m_bucketHasBeenSet = true; m_bucket = value; }

    
    inline void SetBucket(Aws::String&& value) { m_bucketHasBeenSet = true; m_bucket = std::move(value); }

    
    inline void SetBucket(const char* value) { m_bucketHasBeenSet = true; m_bucket.assign(value); }

    
    inline PutBucketLifecycleConfigurationRequest& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    
    inline PutBucketLifecycleConfigurationRequest& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    
    inline PutBucketLifecycleConfigurationRequest& WithBucket(const char* value) { SetBucket(value); return *this;}


    
    inline const BucketLifecycleConfiguration& GetLifecycleConfiguration() const{ return m_lifecycleConfiguration; }

    
    inline void SetLifecycleConfiguration(const BucketLifecycleConfiguration& value) { m_lifecycleConfigurationHasBeenSet = true; m_lifecycleConfiguration = value; }

    
    inline void SetLifecycleConfiguration(BucketLifecycleConfiguration&& value) { m_lifecycleConfigurationHasBeenSet = true; m_lifecycleConfiguration = std::move(value); }

    
    inline PutBucketLifecycleConfigurationRequest& WithLifecycleConfiguration(const BucketLifecycleConfiguration& value) { SetLifecycleConfiguration(value); return *this;}

    
    inline PutBucketLifecycleConfigurationRequest& WithLifecycleConfiguration(BucketLifecycleConfiguration&& value) { SetLifecycleConfiguration(std::move(value)); return *this;}

  private:

    Aws::String m_bucket;
    bool m_bucketHasBeenSet;

    BucketLifecycleConfiguration m_lifecycleConfiguration;
    bool m_lifecycleConfigurationHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
