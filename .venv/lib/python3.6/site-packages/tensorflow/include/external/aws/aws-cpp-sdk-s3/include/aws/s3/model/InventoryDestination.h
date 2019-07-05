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
#include <aws/s3/model/InventoryS3BucketDestination.h>
#include <utility>

namespace Aws
{
namespace Utils
{
namespace Xml
{
  class XmlNode;
} // namespace Xml
} // namespace Utils
namespace S3
{
namespace Model
{

  class AWS_S3_API InventoryDestination
  {
  public:
    InventoryDestination();
    InventoryDestination(const Aws::Utils::Xml::XmlNode& xmlNode);
    InventoryDestination& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * Contains the bucket name, file format, bucket owner (optional), and prefix
     * (optional) where inventory results are published.
     */
    inline const InventoryS3BucketDestination& GetS3BucketDestination() const{ return m_s3BucketDestination; }

    /**
     * Contains the bucket name, file format, bucket owner (optional), and prefix
     * (optional) where inventory results are published.
     */
    inline void SetS3BucketDestination(const InventoryS3BucketDestination& value) { m_s3BucketDestinationHasBeenSet = true; m_s3BucketDestination = value; }

    /**
     * Contains the bucket name, file format, bucket owner (optional), and prefix
     * (optional) where inventory results are published.
     */
    inline void SetS3BucketDestination(InventoryS3BucketDestination&& value) { m_s3BucketDestinationHasBeenSet = true; m_s3BucketDestination = std::move(value); }

    /**
     * Contains the bucket name, file format, bucket owner (optional), and prefix
     * (optional) where inventory results are published.
     */
    inline InventoryDestination& WithS3BucketDestination(const InventoryS3BucketDestination& value) { SetS3BucketDestination(value); return *this;}

    /**
     * Contains the bucket name, file format, bucket owner (optional), and prefix
     * (optional) where inventory results are published.
     */
    inline InventoryDestination& WithS3BucketDestination(InventoryS3BucketDestination&& value) { SetS3BucketDestination(std::move(value)); return *this;}

  private:

    InventoryS3BucketDestination m_s3BucketDestination;
    bool m_s3BucketDestinationHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
