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
#include <aws/core/utils/stream/ResponseStream.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <utility>

namespace Aws
{
template<typename RESULT_TYPE>
class AmazonWebServiceResult;

namespace S3
{
namespace Model
{
  class AWS_S3_API GetBucketPolicyResult
  {
  public:
    GetBucketPolicyResult();
    //We have to define these because Microsoft doesn't auto generate them
    GetBucketPolicyResult(GetBucketPolicyResult&&);
    GetBucketPolicyResult& operator=(GetBucketPolicyResult&&);
    //we delete these because Microsoft doesn't handle move generation correctly
    //and we therefore don't trust them to get it right here either.
    GetBucketPolicyResult(const GetBucketPolicyResult&) = delete;
    GetBucketPolicyResult& operator=(const GetBucketPolicyResult&) = delete;


    GetBucketPolicyResult(Aws::AmazonWebServiceResult<Aws::Utils::Stream::ResponseStream>&& result);
    GetBucketPolicyResult& operator=(Aws::AmazonWebServiceResult<Aws::Utils::Stream::ResponseStream>&& result);



    /**
     * The bucket policy as a JSON document.
     */
    inline Aws::IOStream& GetPolicy() { return m_policy.GetUnderlyingStream(); }

    /**
     * The bucket policy as a JSON document.
     */
    inline void ReplaceBody(Aws::IOStream* body) { m_policy = Aws::Utils::Stream::ResponseStream(body); }
    
  private:

  Aws::Utils::Stream::ResponseStream m_policy;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
