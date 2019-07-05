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
#include <aws/s3/model/InventoryConfiguration.h>
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
  class AWS_S3_API PutBucketInventoryConfigurationRequest : public S3Request
  {
  public:
    PutBucketInventoryConfigurationRequest();
    
    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "PutBucketInventoryConfiguration"; }

    Aws::String SerializePayload() const override;

    void AddQueryStringParameters(Aws::Http::URI& uri) const override;


    /**
     * The name of the bucket where the inventory configuration will be stored.
     */
    inline const Aws::String& GetBucket() const{ return m_bucket; }

    /**
     * The name of the bucket where the inventory configuration will be stored.
     */
    inline void SetBucket(const Aws::String& value) { m_bucketHasBeenSet = true; m_bucket = value; }

    /**
     * The name of the bucket where the inventory configuration will be stored.
     */
    inline void SetBucket(Aws::String&& value) { m_bucketHasBeenSet = true; m_bucket = std::move(value); }

    /**
     * The name of the bucket where the inventory configuration will be stored.
     */
    inline void SetBucket(const char* value) { m_bucketHasBeenSet = true; m_bucket.assign(value); }

    /**
     * The name of the bucket where the inventory configuration will be stored.
     */
    inline PutBucketInventoryConfigurationRequest& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    /**
     * The name of the bucket where the inventory configuration will be stored.
     */
    inline PutBucketInventoryConfigurationRequest& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    /**
     * The name of the bucket where the inventory configuration will be stored.
     */
    inline PutBucketInventoryConfigurationRequest& WithBucket(const char* value) { SetBucket(value); return *this;}


    /**
     * The ID used to identify the inventory configuration.
     */
    inline const Aws::String& GetId() const{ return m_id; }

    /**
     * The ID used to identify the inventory configuration.
     */
    inline void SetId(const Aws::String& value) { m_idHasBeenSet = true; m_id = value; }

    /**
     * The ID used to identify the inventory configuration.
     */
    inline void SetId(Aws::String&& value) { m_idHasBeenSet = true; m_id = std::move(value); }

    /**
     * The ID used to identify the inventory configuration.
     */
    inline void SetId(const char* value) { m_idHasBeenSet = true; m_id.assign(value); }

    /**
     * The ID used to identify the inventory configuration.
     */
    inline PutBucketInventoryConfigurationRequest& WithId(const Aws::String& value) { SetId(value); return *this;}

    /**
     * The ID used to identify the inventory configuration.
     */
    inline PutBucketInventoryConfigurationRequest& WithId(Aws::String&& value) { SetId(std::move(value)); return *this;}

    /**
     * The ID used to identify the inventory configuration.
     */
    inline PutBucketInventoryConfigurationRequest& WithId(const char* value) { SetId(value); return *this;}


    /**
     * Specifies the inventory configuration.
     */
    inline const InventoryConfiguration& GetInventoryConfiguration() const{ return m_inventoryConfiguration; }

    /**
     * Specifies the inventory configuration.
     */
    inline void SetInventoryConfiguration(const InventoryConfiguration& value) { m_inventoryConfigurationHasBeenSet = true; m_inventoryConfiguration = value; }

    /**
     * Specifies the inventory configuration.
     */
    inline void SetInventoryConfiguration(InventoryConfiguration&& value) { m_inventoryConfigurationHasBeenSet = true; m_inventoryConfiguration = std::move(value); }

    /**
     * Specifies the inventory configuration.
     */
    inline PutBucketInventoryConfigurationRequest& WithInventoryConfiguration(const InventoryConfiguration& value) { SetInventoryConfiguration(value); return *this;}

    /**
     * Specifies the inventory configuration.
     */
    inline PutBucketInventoryConfigurationRequest& WithInventoryConfiguration(InventoryConfiguration&& value) { SetInventoryConfiguration(std::move(value)); return *this;}

  private:

    Aws::String m_bucket;
    bool m_bucketHasBeenSet;

    Aws::String m_id;
    bool m_idHasBeenSet;

    InventoryConfiguration m_inventoryConfiguration;
    bool m_inventoryConfigurationHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
