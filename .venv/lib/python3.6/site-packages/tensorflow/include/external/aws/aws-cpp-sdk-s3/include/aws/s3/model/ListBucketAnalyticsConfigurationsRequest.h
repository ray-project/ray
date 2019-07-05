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
#include <utility>

namespace Aws
{
namespace Http
{
    class URI;
} //namespace Http
namespace S3
{
namespace Model
{

  /**
   */
  class AWS_S3_API ListBucketAnalyticsConfigurationsRequest : public S3Request
  {
  public:
    ListBucketAnalyticsConfigurationsRequest();
    
    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "ListBucketAnalyticsConfigurations"; }

    Aws::String SerializePayload() const override;

    void AddQueryStringParameters(Aws::Http::URI& uri) const override;


    /**
     * The name of the bucket from which analytics configurations are retrieved.
     */
    inline const Aws::String& GetBucket() const{ return m_bucket; }

    /**
     * The name of the bucket from which analytics configurations are retrieved.
     */
    inline void SetBucket(const Aws::String& value) { m_bucketHasBeenSet = true; m_bucket = value; }

    /**
     * The name of the bucket from which analytics configurations are retrieved.
     */
    inline void SetBucket(Aws::String&& value) { m_bucketHasBeenSet = true; m_bucket = std::move(value); }

    /**
     * The name of the bucket from which analytics configurations are retrieved.
     */
    inline void SetBucket(const char* value) { m_bucketHasBeenSet = true; m_bucket.assign(value); }

    /**
     * The name of the bucket from which analytics configurations are retrieved.
     */
    inline ListBucketAnalyticsConfigurationsRequest& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    /**
     * The name of the bucket from which analytics configurations are retrieved.
     */
    inline ListBucketAnalyticsConfigurationsRequest& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    /**
     * The name of the bucket from which analytics configurations are retrieved.
     */
    inline ListBucketAnalyticsConfigurationsRequest& WithBucket(const char* value) { SetBucket(value); return *this;}


    /**
     * The ContinuationToken that represents a placeholder from where this request
     * should begin.
     */
    inline const Aws::String& GetContinuationToken() const{ return m_continuationToken; }

    /**
     * The ContinuationToken that represents a placeholder from where this request
     * should begin.
     */
    inline void SetContinuationToken(const Aws::String& value) { m_continuationTokenHasBeenSet = true; m_continuationToken = value; }

    /**
     * The ContinuationToken that represents a placeholder from where this request
     * should begin.
     */
    inline void SetContinuationToken(Aws::String&& value) { m_continuationTokenHasBeenSet = true; m_continuationToken = std::move(value); }

    /**
     * The ContinuationToken that represents a placeholder from where this request
     * should begin.
     */
    inline void SetContinuationToken(const char* value) { m_continuationTokenHasBeenSet = true; m_continuationToken.assign(value); }

    /**
     * The ContinuationToken that represents a placeholder from where this request
     * should begin.
     */
    inline ListBucketAnalyticsConfigurationsRequest& WithContinuationToken(const Aws::String& value) { SetContinuationToken(value); return *this;}

    /**
     * The ContinuationToken that represents a placeholder from where this request
     * should begin.
     */
    inline ListBucketAnalyticsConfigurationsRequest& WithContinuationToken(Aws::String&& value) { SetContinuationToken(std::move(value)); return *this;}

    /**
     * The ContinuationToken that represents a placeholder from where this request
     * should begin.
     */
    inline ListBucketAnalyticsConfigurationsRequest& WithContinuationToken(const char* value) { SetContinuationToken(value); return *this;}

  private:

    Aws::String m_bucket;
    bool m_bucketHasBeenSet;

    Aws::String m_continuationToken;
    bool m_continuationTokenHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
