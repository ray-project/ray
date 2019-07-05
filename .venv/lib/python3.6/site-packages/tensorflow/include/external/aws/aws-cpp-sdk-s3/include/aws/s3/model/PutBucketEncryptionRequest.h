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
#include <aws/s3/model/ServerSideEncryptionConfiguration.h>
#include <utility>

namespace Aws
{
namespace S3
{
namespace Model
{

  /**
   */
  class AWS_S3_API PutBucketEncryptionRequest : public S3Request
  {
  public:
    PutBucketEncryptionRequest();
    
    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "PutBucketEncryption"; }

    Aws::String SerializePayload() const override;

    Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override;


    /**
     * The name of the bucket for which the server-side encryption configuration is
     * set.
     */
    inline const Aws::String& GetBucket() const{ return m_bucket; }

    /**
     * The name of the bucket for which the server-side encryption configuration is
     * set.
     */
    inline void SetBucket(const Aws::String& value) { m_bucketHasBeenSet = true; m_bucket = value; }

    /**
     * The name of the bucket for which the server-side encryption configuration is
     * set.
     */
    inline void SetBucket(Aws::String&& value) { m_bucketHasBeenSet = true; m_bucket = std::move(value); }

    /**
     * The name of the bucket for which the server-side encryption configuration is
     * set.
     */
    inline void SetBucket(const char* value) { m_bucketHasBeenSet = true; m_bucket.assign(value); }

    /**
     * The name of the bucket for which the server-side encryption configuration is
     * set.
     */
    inline PutBucketEncryptionRequest& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    /**
     * The name of the bucket for which the server-side encryption configuration is
     * set.
     */
    inline PutBucketEncryptionRequest& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    /**
     * The name of the bucket for which the server-side encryption configuration is
     * set.
     */
    inline PutBucketEncryptionRequest& WithBucket(const char* value) { SetBucket(value); return *this;}


    /**
     * The base64-encoded 128-bit MD5 digest of the server-side encryption
     * configuration.
     */
    inline const Aws::String& GetContentMD5() const{ return m_contentMD5; }

    /**
     * The base64-encoded 128-bit MD5 digest of the server-side encryption
     * configuration.
     */
    inline void SetContentMD5(const Aws::String& value) { m_contentMD5HasBeenSet = true; m_contentMD5 = value; }

    /**
     * The base64-encoded 128-bit MD5 digest of the server-side encryption
     * configuration.
     */
    inline void SetContentMD5(Aws::String&& value) { m_contentMD5HasBeenSet = true; m_contentMD5 = std::move(value); }

    /**
     * The base64-encoded 128-bit MD5 digest of the server-side encryption
     * configuration.
     */
    inline void SetContentMD5(const char* value) { m_contentMD5HasBeenSet = true; m_contentMD5.assign(value); }

    /**
     * The base64-encoded 128-bit MD5 digest of the server-side encryption
     * configuration.
     */
    inline PutBucketEncryptionRequest& WithContentMD5(const Aws::String& value) { SetContentMD5(value); return *this;}

    /**
     * The base64-encoded 128-bit MD5 digest of the server-side encryption
     * configuration.
     */
    inline PutBucketEncryptionRequest& WithContentMD5(Aws::String&& value) { SetContentMD5(std::move(value)); return *this;}

    /**
     * The base64-encoded 128-bit MD5 digest of the server-side encryption
     * configuration.
     */
    inline PutBucketEncryptionRequest& WithContentMD5(const char* value) { SetContentMD5(value); return *this;}


    
    inline const ServerSideEncryptionConfiguration& GetServerSideEncryptionConfiguration() const{ return m_serverSideEncryptionConfiguration; }

    
    inline void SetServerSideEncryptionConfiguration(const ServerSideEncryptionConfiguration& value) { m_serverSideEncryptionConfigurationHasBeenSet = true; m_serverSideEncryptionConfiguration = value; }

    
    inline void SetServerSideEncryptionConfiguration(ServerSideEncryptionConfiguration&& value) { m_serverSideEncryptionConfigurationHasBeenSet = true; m_serverSideEncryptionConfiguration = std::move(value); }

    
    inline PutBucketEncryptionRequest& WithServerSideEncryptionConfiguration(const ServerSideEncryptionConfiguration& value) { SetServerSideEncryptionConfiguration(value); return *this;}

    
    inline PutBucketEncryptionRequest& WithServerSideEncryptionConfiguration(ServerSideEncryptionConfiguration&& value) { SetServerSideEncryptionConfiguration(std::move(value)); return *this;}

  private:

    Aws::String m_bucket;
    bool m_bucketHasBeenSet;

    Aws::String m_contentMD5;
    bool m_contentMD5HasBeenSet;

    ServerSideEncryptionConfiguration m_serverSideEncryptionConfiguration;
    bool m_serverSideEncryptionConfigurationHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
