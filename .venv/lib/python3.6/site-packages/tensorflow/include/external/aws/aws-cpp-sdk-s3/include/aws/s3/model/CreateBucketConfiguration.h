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
#include <aws/s3/model/BucketLocationConstraint.h>
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

  class AWS_S3_API CreateBucketConfiguration
  {
  public:
    CreateBucketConfiguration();
    CreateBucketConfiguration(const Aws::Utils::Xml::XmlNode& xmlNode);
    CreateBucketConfiguration& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * Specifies the region where the bucket will be created. If you don't specify a
     * region, the bucket will be created in US Standard.
     */
    inline const BucketLocationConstraint& GetLocationConstraint() const{ return m_locationConstraint; }

    /**
     * Specifies the region where the bucket will be created. If you don't specify a
     * region, the bucket will be created in US Standard.
     */
    inline void SetLocationConstraint(const BucketLocationConstraint& value) { m_locationConstraintHasBeenSet = true; m_locationConstraint = value; }

    /**
     * Specifies the region where the bucket will be created. If you don't specify a
     * region, the bucket will be created in US Standard.
     */
    inline void SetLocationConstraint(BucketLocationConstraint&& value) { m_locationConstraintHasBeenSet = true; m_locationConstraint = std::move(value); }

    /**
     * Specifies the region where the bucket will be created. If you don't specify a
     * region, the bucket will be created in US Standard.
     */
    inline CreateBucketConfiguration& WithLocationConstraint(const BucketLocationConstraint& value) { SetLocationConstraint(value); return *this;}

    /**
     * Specifies the region where the bucket will be created. If you don't specify a
     * region, the bucket will be created in US Standard.
     */
    inline CreateBucketConfiguration& WithLocationConstraint(BucketLocationConstraint&& value) { SetLocationConstraint(std::move(value)); return *this;}

  private:

    BucketLocationConstraint m_locationConstraint;
    bool m_locationConstraintHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
