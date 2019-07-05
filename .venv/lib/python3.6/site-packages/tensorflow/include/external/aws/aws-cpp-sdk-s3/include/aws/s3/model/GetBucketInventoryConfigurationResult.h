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
#include <aws/s3/model/InventoryConfiguration.h>
#include <utility>

namespace Aws
{
template<typename RESULT_TYPE>
class AmazonWebServiceResult;

namespace Utils
{
namespace Xml
{
  class XmlDocument;
} // namespace Xml
} // namespace Utils
namespace S3
{
namespace Model
{
  class AWS_S3_API GetBucketInventoryConfigurationResult
  {
  public:
    GetBucketInventoryConfigurationResult();
    GetBucketInventoryConfigurationResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    GetBucketInventoryConfigurationResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * Specifies the inventory configuration.
     */
    inline const InventoryConfiguration& GetInventoryConfiguration() const{ return m_inventoryConfiguration; }

    /**
     * Specifies the inventory configuration.
     */
    inline void SetInventoryConfiguration(const InventoryConfiguration& value) { m_inventoryConfiguration = value; }

    /**
     * Specifies the inventory configuration.
     */
    inline void SetInventoryConfiguration(InventoryConfiguration&& value) { m_inventoryConfiguration = std::move(value); }

    /**
     * Specifies the inventory configuration.
     */
    inline GetBucketInventoryConfigurationResult& WithInventoryConfiguration(const InventoryConfiguration& value) { SetInventoryConfiguration(value); return *this;}

    /**
     * Specifies the inventory configuration.
     */
    inline GetBucketInventoryConfigurationResult& WithInventoryConfiguration(InventoryConfiguration&& value) { SetInventoryConfiguration(std::move(value)); return *this;}

  private:

    InventoryConfiguration m_inventoryConfiguration;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
