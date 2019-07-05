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
#include <aws/s3/model/AnalyticsConfiguration.h>
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
  class AWS_S3_API PutBucketAnalyticsConfigurationRequest : public S3Request
  {
  public:
    PutBucketAnalyticsConfigurationRequest();
    
    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "PutBucketAnalyticsConfiguration"; }

    Aws::String SerializePayload() const override;

    void AddQueryStringParameters(Aws::Http::URI& uri) const override;


    /**
     * The name of the bucket to which an analytics configuration is stored.
     */
    inline const Aws::String& GetBucket() const{ return m_bucket; }

    /**
     * The name of the bucket to which an analytics configuration is stored.
     */
    inline void SetBucket(const Aws::String& value) { m_bucketHasBeenSet = true; m_bucket = value; }

    /**
     * The name of the bucket to which an analytics configuration is stored.
     */
    inline void SetBucket(Aws::String&& value) { m_bucketHasBeenSet = true; m_bucket = std::move(value); }

    /**
     * The name of the bucket to which an analytics configuration is stored.
     */
    inline void SetBucket(const char* value) { m_bucketHasBeenSet = true; m_bucket.assign(value); }

    /**
     * The name of the bucket to which an analytics configuration is stored.
     */
    inline PutBucketAnalyticsConfigurationRequest& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    /**
     * The name of the bucket to which an analytics configuration is stored.
     */
    inline PutBucketAnalyticsConfigurationRequest& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    /**
     * The name of the bucket to which an analytics configuration is stored.
     */
    inline PutBucketAnalyticsConfigurationRequest& WithBucket(const char* value) { SetBucket(value); return *this;}


    /**
     * The identifier used to represent an analytics configuration.
     */
    inline const Aws::String& GetId() const{ return m_id; }

    /**
     * The identifier used to represent an analytics configuration.
     */
    inline void SetId(const Aws::String& value) { m_idHasBeenSet = true; m_id = value; }

    /**
     * The identifier used to represent an analytics configuration.
     */
    inline void SetId(Aws::String&& value) { m_idHasBeenSet = true; m_id = std::move(value); }

    /**
     * The identifier used to represent an analytics configuration.
     */
    inline void SetId(const char* value) { m_idHasBeenSet = true; m_id.assign(value); }

    /**
     * The identifier used to represent an analytics configuration.
     */
    inline PutBucketAnalyticsConfigurationRequest& WithId(const Aws::String& value) { SetId(value); return *this;}

    /**
     * The identifier used to represent an analytics configuration.
     */
    inline PutBucketAnalyticsConfigurationRequest& WithId(Aws::String&& value) { SetId(std::move(value)); return *this;}

    /**
     * The identifier used to represent an analytics configuration.
     */
    inline PutBucketAnalyticsConfigurationRequest& WithId(const char* value) { SetId(value); return *this;}


    /**
     * The configuration and any analyses for the analytics filter.
     */
    inline const AnalyticsConfiguration& GetAnalyticsConfiguration() const{ return m_analyticsConfiguration; }

    /**
     * The configuration and any analyses for the analytics filter.
     */
    inline void SetAnalyticsConfiguration(const AnalyticsConfiguration& value) { m_analyticsConfigurationHasBeenSet = true; m_analyticsConfiguration = value; }

    /**
     * The configuration and any analyses for the analytics filter.
     */
    inline void SetAnalyticsConfiguration(AnalyticsConfiguration&& value) { m_analyticsConfigurationHasBeenSet = true; m_analyticsConfiguration = std::move(value); }

    /**
     * The configuration and any analyses for the analytics filter.
     */
    inline PutBucketAnalyticsConfigurationRequest& WithAnalyticsConfiguration(const AnalyticsConfiguration& value) { SetAnalyticsConfiguration(value); return *this;}

    /**
     * The configuration and any analyses for the analytics filter.
     */
    inline PutBucketAnalyticsConfigurationRequest& WithAnalyticsConfiguration(AnalyticsConfiguration&& value) { SetAnalyticsConfiguration(std::move(value)); return *this;}

  private:

    Aws::String m_bucket;
    bool m_bucketHasBeenSet;

    Aws::String m_id;
    bool m_idHasBeenSet;

    AnalyticsConfiguration m_analyticsConfiguration;
    bool m_analyticsConfigurationHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
