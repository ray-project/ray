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
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/s3/model/CORSRule.h>
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
  class AWS_S3_API GetBucketCorsResult
  {
  public:
    GetBucketCorsResult();
    GetBucketCorsResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    GetBucketCorsResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    
    inline const Aws::Vector<CORSRule>& GetCORSRules() const{ return m_cORSRules; }

    
    inline void SetCORSRules(const Aws::Vector<CORSRule>& value) { m_cORSRules = value; }

    
    inline void SetCORSRules(Aws::Vector<CORSRule>&& value) { m_cORSRules = std::move(value); }

    
    inline GetBucketCorsResult& WithCORSRules(const Aws::Vector<CORSRule>& value) { SetCORSRules(value); return *this;}

    
    inline GetBucketCorsResult& WithCORSRules(Aws::Vector<CORSRule>&& value) { SetCORSRules(std::move(value)); return *this;}

    
    inline GetBucketCorsResult& AddCORSRules(const CORSRule& value) { m_cORSRules.push_back(value); return *this; }

    
    inline GetBucketCorsResult& AddCORSRules(CORSRule&& value) { m_cORSRules.push_back(std::move(value)); return *this; }

  private:

    Aws::Vector<CORSRule> m_cORSRules;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
