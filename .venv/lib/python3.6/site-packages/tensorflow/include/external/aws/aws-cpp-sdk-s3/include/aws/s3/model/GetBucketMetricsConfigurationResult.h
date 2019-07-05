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
#include <aws/s3/model/MetricsConfiguration.h>
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
  class AWS_S3_API GetBucketMetricsConfigurationResult
  {
  public:
    GetBucketMetricsConfigurationResult();
    GetBucketMetricsConfigurationResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    GetBucketMetricsConfigurationResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * Specifies the metrics configuration.
     */
    inline const MetricsConfiguration& GetMetricsConfiguration() const{ return m_metricsConfiguration; }

    /**
     * Specifies the metrics configuration.
     */
    inline void SetMetricsConfiguration(const MetricsConfiguration& value) { m_metricsConfiguration = value; }

    /**
     * Specifies the metrics configuration.
     */
    inline void SetMetricsConfiguration(MetricsConfiguration&& value) { m_metricsConfiguration = std::move(value); }

    /**
     * Specifies the metrics configuration.
     */
    inline GetBucketMetricsConfigurationResult& WithMetricsConfiguration(const MetricsConfiguration& value) { SetMetricsConfiguration(value); return *this;}

    /**
     * Specifies the metrics configuration.
     */
    inline GetBucketMetricsConfigurationResult& WithMetricsConfiguration(MetricsConfiguration&& value) { SetMetricsConfiguration(std::move(value)); return *this;}

  private:

    MetricsConfiguration m_metricsConfiguration;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
