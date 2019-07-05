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
#include <aws/s3/model/RequestPaymentConfiguration.h>
#include <utility>

namespace Aws
{
namespace S3
{
namespace Model
{

  /**
   */
  class AWS_S3_API PutBucketRequestPaymentRequest : public S3Request
  {
  public:
    PutBucketRequestPaymentRequest();
    
    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "PutBucketRequestPayment"; }

    Aws::String SerializePayload() const override;

    Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override;


    
    inline const Aws::String& GetBucket() const{ return m_bucket; }

    
    inline void SetBucket(const Aws::String& value) { m_bucketHasBeenSet = true; m_bucket = value; }

    
    inline void SetBucket(Aws::String&& value) { m_bucketHasBeenSet = true; m_bucket = std::move(value); }

    
    inline void SetBucket(const char* value) { m_bucketHasBeenSet = true; m_bucket.assign(value); }

    
    inline PutBucketRequestPaymentRequest& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    
    inline PutBucketRequestPaymentRequest& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    
    inline PutBucketRequestPaymentRequest& WithBucket(const char* value) { SetBucket(value); return *this;}


    
    inline const Aws::String& GetContentMD5() const{ return m_contentMD5; }

    
    inline void SetContentMD5(const Aws::String& value) { m_contentMD5HasBeenSet = true; m_contentMD5 = value; }

    
    inline void SetContentMD5(Aws::String&& value) { m_contentMD5HasBeenSet = true; m_contentMD5 = std::move(value); }

    
    inline void SetContentMD5(const char* value) { m_contentMD5HasBeenSet = true; m_contentMD5.assign(value); }

    
    inline PutBucketRequestPaymentRequest& WithContentMD5(const Aws::String& value) { SetContentMD5(value); return *this;}

    
    inline PutBucketRequestPaymentRequest& WithContentMD5(Aws::String&& value) { SetContentMD5(std::move(value)); return *this;}

    
    inline PutBucketRequestPaymentRequest& WithContentMD5(const char* value) { SetContentMD5(value); return *this;}


    
    inline const RequestPaymentConfiguration& GetRequestPaymentConfiguration() const{ return m_requestPaymentConfiguration; }

    
    inline void SetRequestPaymentConfiguration(const RequestPaymentConfiguration& value) { m_requestPaymentConfigurationHasBeenSet = true; m_requestPaymentConfiguration = value; }

    
    inline void SetRequestPaymentConfiguration(RequestPaymentConfiguration&& value) { m_requestPaymentConfigurationHasBeenSet = true; m_requestPaymentConfiguration = std::move(value); }

    
    inline PutBucketRequestPaymentRequest& WithRequestPaymentConfiguration(const RequestPaymentConfiguration& value) { SetRequestPaymentConfiguration(value); return *this;}

    
    inline PutBucketRequestPaymentRequest& WithRequestPaymentConfiguration(RequestPaymentConfiguration&& value) { SetRequestPaymentConfiguration(std::move(value)); return *this;}

  private:

    Aws::String m_bucket;
    bool m_bucketHasBeenSet;

    Aws::String m_contentMD5;
    bool m_contentMD5HasBeenSet;

    RequestPaymentConfiguration m_requestPaymentConfiguration;
    bool m_requestPaymentConfigurationHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
