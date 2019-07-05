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
#include <aws/s3/model/AnalyticsS3BucketDestination.h>
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

  class AWS_S3_API AnalyticsExportDestination
  {
  public:
    AnalyticsExportDestination();
    AnalyticsExportDestination(const Aws::Utils::Xml::XmlNode& xmlNode);
    AnalyticsExportDestination& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * A destination signifying output to an S3 bucket.
     */
    inline const AnalyticsS3BucketDestination& GetS3BucketDestination() const{ return m_s3BucketDestination; }

    /**
     * A destination signifying output to an S3 bucket.
     */
    inline void SetS3BucketDestination(const AnalyticsS3BucketDestination& value) { m_s3BucketDestinationHasBeenSet = true; m_s3BucketDestination = value; }

    /**
     * A destination signifying output to an S3 bucket.
     */
    inline void SetS3BucketDestination(AnalyticsS3BucketDestination&& value) { m_s3BucketDestinationHasBeenSet = true; m_s3BucketDestination = std::move(value); }

    /**
     * A destination signifying output to an S3 bucket.
     */
    inline AnalyticsExportDestination& WithS3BucketDestination(const AnalyticsS3BucketDestination& value) { SetS3BucketDestination(value); return *this;}

    /**
     * A destination signifying output to an S3 bucket.
     */
    inline AnalyticsExportDestination& WithS3BucketDestination(AnalyticsS3BucketDestination&& value) { SetS3BucketDestination(std::move(value)); return *this;}

  private:

    AnalyticsS3BucketDestination m_s3BucketDestination;
    bool m_s3BucketDestinationHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
