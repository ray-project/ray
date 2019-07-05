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
#include <aws/s3/model/Owner.h>
#include <aws/s3/model/Bucket.h>
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
  class AWS_S3_API ListBucketsResult
  {
  public:
    ListBucketsResult();
    ListBucketsResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    ListBucketsResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    
    inline const Aws::Vector<Bucket>& GetBuckets() const{ return m_buckets; }

    
    inline void SetBuckets(const Aws::Vector<Bucket>& value) { m_buckets = value; }

    
    inline void SetBuckets(Aws::Vector<Bucket>&& value) { m_buckets = std::move(value); }

    
    inline ListBucketsResult& WithBuckets(const Aws::Vector<Bucket>& value) { SetBuckets(value); return *this;}

    
    inline ListBucketsResult& WithBuckets(Aws::Vector<Bucket>&& value) { SetBuckets(std::move(value)); return *this;}

    
    inline ListBucketsResult& AddBuckets(const Bucket& value) { m_buckets.push_back(value); return *this; }

    
    inline ListBucketsResult& AddBuckets(Bucket&& value) { m_buckets.push_back(std::move(value)); return *this; }


    
    inline const Owner& GetOwner() const{ return m_owner; }

    
    inline void SetOwner(const Owner& value) { m_owner = value; }

    
    inline void SetOwner(Owner&& value) { m_owner = std::move(value); }

    
    inline ListBucketsResult& WithOwner(const Owner& value) { SetOwner(value); return *this;}

    
    inline ListBucketsResult& WithOwner(Owner&& value) { SetOwner(std::move(value)); return *this;}

  private:

    Aws::Vector<Bucket> m_buckets;

    Owner m_owner;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
