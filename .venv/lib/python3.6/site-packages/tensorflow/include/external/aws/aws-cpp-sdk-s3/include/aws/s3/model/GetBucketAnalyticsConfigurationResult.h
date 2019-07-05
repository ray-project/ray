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
#include <aws/s3/model/AnalyticsConfiguration.h>
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
  class AWS_S3_API GetBucketAnalyticsConfigurationResult
  {
  public:
    GetBucketAnalyticsConfigurationResult();
    GetBucketAnalyticsConfigurationResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    GetBucketAnalyticsConfigurationResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * The configuration and any analyses for the analytics filter.
     */
    inline const AnalyticsConfiguration& GetAnalyticsConfiguration() const{ return m_analyticsConfiguration; }

    /**
     * The configuration and any analyses for the analytics filter.
     */
    inline void SetAnalyticsConfiguration(const AnalyticsConfiguration& value) { m_analyticsConfiguration = value; }

    /**
     * The configuration and any analyses for the analytics filter.
     */
    inline void SetAnalyticsConfiguration(AnalyticsConfiguration&& value) { m_analyticsConfiguration = std::move(value); }

    /**
     * The configuration and any analyses for the analytics filter.
     */
    inline GetBucketAnalyticsConfigurationResult& WithAnalyticsConfiguration(const AnalyticsConfiguration& value) { SetAnalyticsConfiguration(value); return *this;}

    /**
     * The configuration and any analyses for the analytics filter.
     */
    inline GetBucketAnalyticsConfigurationResult& WithAnalyticsConfiguration(AnalyticsConfiguration&& value) { SetAnalyticsConfiguration(std::move(value)); return *this;}

  private:

    AnalyticsConfiguration m_analyticsConfiguration;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
