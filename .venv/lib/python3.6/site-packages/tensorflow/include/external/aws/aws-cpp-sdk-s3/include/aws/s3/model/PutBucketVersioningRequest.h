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
#include <aws/s3/model/VersioningConfiguration.h>
#include <utility>

namespace Aws
{
namespace S3
{
namespace Model
{

  /**
   */
  class AWS_S3_API PutBucketVersioningRequest : public S3Request
  {
  public:
    PutBucketVersioningRequest();
    
    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "PutBucketVersioning"; }

    Aws::String SerializePayload() const override;

    Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override;


    
    inline const Aws::String& GetBucket() const{ return m_bucket; }

    
    inline void SetBucket(const Aws::String& value) { m_bucketHasBeenSet = true; m_bucket = value; }

    
    inline void SetBucket(Aws::String&& value) { m_bucketHasBeenSet = true; m_bucket = std::move(value); }

    
    inline void SetBucket(const char* value) { m_bucketHasBeenSet = true; m_bucket.assign(value); }

    
    inline PutBucketVersioningRequest& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    
    inline PutBucketVersioningRequest& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    
    inline PutBucketVersioningRequest& WithBucket(const char* value) { SetBucket(value); return *this;}


    
    inline const Aws::String& GetContentMD5() const{ return m_contentMD5; }

    
    inline void SetContentMD5(const Aws::String& value) { m_contentMD5HasBeenSet = true; m_contentMD5 = value; }

    
    inline void SetContentMD5(Aws::String&& value) { m_contentMD5HasBeenSet = true; m_contentMD5 = std::move(value); }

    
    inline void SetContentMD5(const char* value) { m_contentMD5HasBeenSet = true; m_contentMD5.assign(value); }

    
    inline PutBucketVersioningRequest& WithContentMD5(const Aws::String& value) { SetContentMD5(value); return *this;}

    
    inline PutBucketVersioningRequest& WithContentMD5(Aws::String&& value) { SetContentMD5(std::move(value)); return *this;}

    
    inline PutBucketVersioningRequest& WithContentMD5(const char* value) { SetContentMD5(value); return *this;}


    /**
     * The concatenation of the authentication device's serial number, a space, and the
     * value that is displayed on your authentication device.
     */
    inline const Aws::String& GetMFA() const{ return m_mFA; }

    /**
     * The concatenation of the authentication device's serial number, a space, and the
     * value that is displayed on your authentication device.
     */
    inline void SetMFA(const Aws::String& value) { m_mFAHasBeenSet = true; m_mFA = value; }

    /**
     * The concatenation of the authentication device's serial number, a space, and the
     * value that is displayed on your authentication device.
     */
    inline void SetMFA(Aws::String&& value) { m_mFAHasBeenSet = true; m_mFA = std::move(value); }

    /**
     * The concatenation of the authentication device's serial number, a space, and the
     * value that is displayed on your authentication device.
     */
    inline void SetMFA(const char* value) { m_mFAHasBeenSet = true; m_mFA.assign(value); }

    /**
     * The concatenation of the authentication device's serial number, a space, and the
     * value that is displayed on your authentication device.
     */
    inline PutBucketVersioningRequest& WithMFA(const Aws::String& value) { SetMFA(value); return *this;}

    /**
     * The concatenation of the authentication device's serial number, a space, and the
     * value that is displayed on your authentication device.
     */
    inline PutBucketVersioningRequest& WithMFA(Aws::String&& value) { SetMFA(std::move(value)); return *this;}

    /**
     * The concatenation of the authentication device's serial number, a space, and the
     * value that is displayed on your authentication device.
     */
    inline PutBucketVersioningRequest& WithMFA(const char* value) { SetMFA(value); return *this;}


    
    inline const VersioningConfiguration& GetVersioningConfiguration() const{ return m_versioningConfiguration; }

    
    inline void SetVersioningConfiguration(const VersioningConfiguration& value) { m_versioningConfigurationHasBeenSet = true; m_versioningConfiguration = value; }

    
    inline void SetVersioningConfiguration(VersioningConfiguration&& value) { m_versioningConfigurationHasBeenSet = true; m_versioningConfiguration = std::move(value); }

    
    inline PutBucketVersioningRequest& WithVersioningConfiguration(const VersioningConfiguration& value) { SetVersioningConfiguration(value); return *this;}

    
    inline PutBucketVersioningRequest& WithVersioningConfiguration(VersioningConfiguration&& value) { SetVersioningConfiguration(std::move(value)); return *this;}

  private:

    Aws::String m_bucket;
    bool m_bucketHasBeenSet;

    Aws::String m_contentMD5;
    bool m_contentMD5HasBeenSet;

    Aws::String m_mFA;
    bool m_mFAHasBeenSet;

    VersioningConfiguration m_versioningConfiguration;
    bool m_versioningConfigurationHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
