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
  class AWS_S3_API ListBucketMetricsConfigurationsRequest : public S3Request
  {
  public:
    ListBucketMetricsConfigurationsRequest();
    
    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "ListBucketMetricsConfigurations"; }

    Aws::String SerializePayload() const override;

    void AddQueryStringParameters(Aws::Http::URI& uri) const override;


    /**
     * The name of the bucket containing the metrics configurations to retrieve.
     */
    inline const Aws::String& GetBucket() const{ return m_bucket; }

    /**
     * The name of the bucket containing the metrics configurations to retrieve.
     */
    inline void SetBucket(const Aws::String& value) { m_bucketHasBeenSet = true; m_bucket = value; }

    /**
     * The name of the bucket containing the metrics configurations to retrieve.
     */
    inline void SetBucket(Aws::String&& value) { m_bucketHasBeenSet = true; m_bucket = std::move(value); }

    /**
     * The name of the bucket containing the metrics configurations to retrieve.
     */
    inline void SetBucket(const char* value) { m_bucketHasBeenSet = true; m_bucket.assign(value); }

    /**
     * The name of the bucket containing the metrics configurations to retrieve.
     */
    inline ListBucketMetricsConfigurationsRequest& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    /**
     * The name of the bucket containing the metrics configurations to retrieve.
     */
    inline ListBucketMetricsConfigurationsRequest& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    /**
     * The name of the bucket containing the metrics configurations to retrieve.
     */
    inline ListBucketMetricsConfigurationsRequest& WithBucket(const char* value) { SetBucket(value); return *this;}


    /**
     * The marker that is used to continue a metrics configuration listing that has
     * been truncated. Use the NextContinuationToken from a previously truncated list
     * response to continue the listing. The continuation token is an opaque value that
     * Amazon S3 understands.
     */
    inline const Aws::String& GetContinuationToken() const{ return m_continuationToken; }

    /**
     * The marker that is used to continue a metrics configuration listing that has
     * been truncated. Use the NextContinuationToken from a previously truncated list
     * response to continue the listing. The continuation token is an opaque value that
     * Amazon S3 understands.
     */
    inline void SetContinuationToken(const Aws::String& value) { m_continuationTokenHasBeenSet = true; m_continuationToken = value; }

    /**
     * The marker that is used to continue a metrics configuration listing that has
     * been truncated. Use the NextContinuationToken from a previously truncated list
     * response to continue the listing. The continuation token is an opaque value that
     * Amazon S3 understands.
     */
    inline void SetContinuationToken(Aws::String&& value) { m_continuationTokenHasBeenSet = true; m_continuationToken = std::move(value); }

    /**
     * The marker that is used to continue a metrics configuration listing that has
     * been truncated. Use the NextContinuationToken from a previously truncated list
     * response to continue the listing. The continuation token is an opaque value that
     * Amazon S3 understands.
     */
    inline void SetContinuationToken(const char* value) { m_continuationTokenHasBeenSet = true; m_continuationToken.assign(value); }

    /**
     * The marker that is used to continue a metrics configuration listing that has
     * been truncated. Use the NextContinuationToken from a previously truncated list
     * response to continue the listing. The continuation token is an opaque value that
     * Amazon S3 understands.
     */
    inline ListBucketMetricsConfigurationsRequest& WithContinuationToken(const Aws::String& value) { SetContinuationToken(value); return *this;}

    /**
     * The marker that is used to continue a metrics configuration listing that has
     * been truncated. Use the NextContinuationToken from a previously truncated list
     * response to continue the listing. The continuation token is an opaque value that
     * Amazon S3 understands.
     */
    inline ListBucketMetricsConfigurationsRequest& WithContinuationToken(Aws::String&& value) { SetContinuationToken(std::move(value)); return *this;}

    /**
     * The marker that is used to continue a metrics configuration listing that has
     * been truncated. Use the NextContinuationToken from a previously truncated list
     * response to continue the listing. The continuation token is an opaque value that
     * Amazon S3 understands.
     */
    inline ListBucketMetricsConfigurationsRequest& WithContinuationToken(const char* value) { SetContinuationToken(value); return *this;}

  private:

    Aws::String m_bucket;
    bool m_bucketHasBeenSet;

    Aws::String m_continuationToken;
    bool m_continuationTokenHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
