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
#include <aws/s3/model/LoggingEnabled.h>
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
  class AWS_S3_API GetBucketLoggingResult
  {
  public:
    GetBucketLoggingResult();
    GetBucketLoggingResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    GetBucketLoggingResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    
    inline const LoggingEnabled& GetLoggingEnabled() const{ return m_loggingEnabled; }

    
    inline void SetLoggingEnabled(const LoggingEnabled& value) { m_loggingEnabled = value; }

    
    inline void SetLoggingEnabled(LoggingEnabled&& value) { m_loggingEnabled = std::move(value); }

    
    inline GetBucketLoggingResult& WithLoggingEnabled(const LoggingEnabled& value) { SetLoggingEnabled(value); return *this;}

    
    inline GetBucketLoggingResult& WithLoggingEnabled(LoggingEnabled&& value) { SetLoggingEnabled(std::move(value)); return *this;}

  private:

    LoggingEnabled m_loggingEnabled;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
