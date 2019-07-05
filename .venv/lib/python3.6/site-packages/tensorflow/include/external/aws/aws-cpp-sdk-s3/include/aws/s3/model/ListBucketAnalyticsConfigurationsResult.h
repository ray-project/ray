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
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
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
  class AWS_S3_API ListBucketAnalyticsConfigurationsResult
  {
  public:
    ListBucketAnalyticsConfigurationsResult();
    ListBucketAnalyticsConfigurationsResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    ListBucketAnalyticsConfigurationsResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * Indicates whether the returned list of analytics configurations is complete. A
     * value of true indicates that the list is not complete and the
     * NextContinuationToken will be provided for a subsequent request.
     */
    inline bool GetIsTruncated() const{ return m_isTruncated; }

    /**
     * Indicates whether the returned list of analytics configurations is complete. A
     * value of true indicates that the list is not complete and the
     * NextContinuationToken will be provided for a subsequent request.
     */
    inline void SetIsTruncated(bool value) { m_isTruncated = value; }

    /**
     * Indicates whether the returned list of analytics configurations is complete. A
     * value of true indicates that the list is not complete and the
     * NextContinuationToken will be provided for a subsequent request.
     */
    inline ListBucketAnalyticsConfigurationsResult& WithIsTruncated(bool value) { SetIsTruncated(value); return *this;}


    /**
     * The ContinuationToken that represents where this request began.
     */
    inline const Aws::String& GetContinuationToken() const{ return m_continuationToken; }

    /**
     * The ContinuationToken that represents where this request began.
     */
    inline void SetContinuationToken(const Aws::String& value) { m_continuationToken = value; }

    /**
     * The ContinuationToken that represents where this request began.
     */
    inline void SetContinuationToken(Aws::String&& value) { m_continuationToken = std::move(value); }

    /**
     * The ContinuationToken that represents where this request began.
     */
    inline void SetContinuationToken(const char* value) { m_continuationToken.assign(value); }

    /**
     * The ContinuationToken that represents where this request began.
     */
    inline ListBucketAnalyticsConfigurationsResult& WithContinuationToken(const Aws::String& value) { SetContinuationToken(value); return *this;}

    /**
     * The ContinuationToken that represents where this request began.
     */
    inline ListBucketAnalyticsConfigurationsResult& WithContinuationToken(Aws::String&& value) { SetContinuationToken(std::move(value)); return *this;}

    /**
     * The ContinuationToken that represents where this request began.
     */
    inline ListBucketAnalyticsConfigurationsResult& WithContinuationToken(const char* value) { SetContinuationToken(value); return *this;}


    /**
     * NextContinuationToken is sent when isTruncated is true, which indicates that
     * there are more analytics configurations to list. The next request must include
     * this NextContinuationToken. The token is obfuscated and is not a usable value.
     */
    inline const Aws::String& GetNextContinuationToken() const{ return m_nextContinuationToken; }

    /**
     * NextContinuationToken is sent when isTruncated is true, which indicates that
     * there are more analytics configurations to list. The next request must include
     * this NextContinuationToken. The token is obfuscated and is not a usable value.
     */
    inline void SetNextContinuationToken(const Aws::String& value) { m_nextContinuationToken = value; }

    /**
     * NextContinuationToken is sent when isTruncated is true, which indicates that
     * there are more analytics configurations to list. The next request must include
     * this NextContinuationToken. The token is obfuscated and is not a usable value.
     */
    inline void SetNextContinuationToken(Aws::String&& value) { m_nextContinuationToken = std::move(value); }

    /**
     * NextContinuationToken is sent when isTruncated is true, which indicates that
     * there are more analytics configurations to list. The next request must include
     * this NextContinuationToken. The token is obfuscated and is not a usable value.
     */
    inline void SetNextContinuationToken(const char* value) { m_nextContinuationToken.assign(value); }

    /**
     * NextContinuationToken is sent when isTruncated is true, which indicates that
     * there are more analytics configurations to list. The next request must include
     * this NextContinuationToken. The token is obfuscated and is not a usable value.
     */
    inline ListBucketAnalyticsConfigurationsResult& WithNextContinuationToken(const Aws::String& value) { SetNextContinuationToken(value); return *this;}

    /**
     * NextContinuationToken is sent when isTruncated is true, which indicates that
     * there are more analytics configurations to list. The next request must include
     * this NextContinuationToken. The token is obfuscated and is not a usable value.
     */
    inline ListBucketAnalyticsConfigurationsResult& WithNextContinuationToken(Aws::String&& value) { SetNextContinuationToken(std::move(value)); return *this;}

    /**
     * NextContinuationToken is sent when isTruncated is true, which indicates that
     * there are more analytics configurations to list. The next request must include
     * this NextContinuationToken. The token is obfuscated and is not a usable value.
     */
    inline ListBucketAnalyticsConfigurationsResult& WithNextContinuationToken(const char* value) { SetNextContinuationToken(value); return *this;}


    /**
     * The list of analytics configurations for a bucket.
     */
    inline const Aws::Vector<AnalyticsConfiguration>& GetAnalyticsConfigurationList() const{ return m_analyticsConfigurationList; }

    /**
     * The list of analytics configurations for a bucket.
     */
    inline void SetAnalyticsConfigurationList(const Aws::Vector<AnalyticsConfiguration>& value) { m_analyticsConfigurationList = value; }

    /**
     * The list of analytics configurations for a bucket.
     */
    inline void SetAnalyticsConfigurationList(Aws::Vector<AnalyticsConfiguration>&& value) { m_analyticsConfigurationList = std::move(value); }

    /**
     * The list of analytics configurations for a bucket.
     */
    inline ListBucketAnalyticsConfigurationsResult& WithAnalyticsConfigurationList(const Aws::Vector<AnalyticsConfiguration>& value) { SetAnalyticsConfigurationList(value); return *this;}

    /**
     * The list of analytics configurations for a bucket.
     */
    inline ListBucketAnalyticsConfigurationsResult& WithAnalyticsConfigurationList(Aws::Vector<AnalyticsConfiguration>&& value) { SetAnalyticsConfigurationList(std::move(value)); return *this;}

    /**
     * The list of analytics configurations for a bucket.
     */
    inline ListBucketAnalyticsConfigurationsResult& AddAnalyticsConfigurationList(const AnalyticsConfiguration& value) { m_analyticsConfigurationList.push_back(value); return *this; }

    /**
     * The list of analytics configurations for a bucket.
     */
    inline ListBucketAnalyticsConfigurationsResult& AddAnalyticsConfigurationList(AnalyticsConfiguration&& value) { m_analyticsConfigurationList.push_back(std::move(value)); return *this; }

  private:

    bool m_isTruncated;

    Aws::String m_continuationToken;

    Aws::String m_nextContinuationToken;

    Aws::Vector<AnalyticsConfiguration> m_analyticsConfigurationList;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
