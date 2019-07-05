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
#include <aws/core/utils/Array.h>
#include <aws/s3/model/RequestCharged.h>
#include <utility>

namespace Aws
{
template<typename RESULT_TYPE>
class AmazonWebServiceResult;

namespace S3
{
namespace Model
{
  class AWS_S3_API GetObjectTorrentResult
  {
  public:
    GetObjectTorrentResult();
    //We have to define these because Microsoft doesn't auto generate them
    GetObjectTorrentResult(GetObjectTorrentResult&&);
    GetObjectTorrentResult& operator=(GetObjectTorrentResult&&);
    //we delete these because Microsoft doesn't handle move generation correctly
    //and we therefore don't trust them to get it right here either.
    GetObjectTorrentResult(const GetObjectTorrentResult&) = delete;
    GetObjectTorrentResult& operator=(const GetObjectTorrentResult&) = delete;


    GetObjectTorrentResult(Aws::AmazonWebServiceResult<Aws::Utils::Stream::ResponseStream>&& result);
    GetObjectTorrentResult& operator=(Aws::AmazonWebServiceResult<Aws::Utils::Stream::ResponseStream>&& result);



    
    inline Aws::IOStream& GetBody() { return m_body.GetUnderlyingStream(); }

    
    inline void ReplaceBody(Aws::IOStream* body) { m_body = Aws::Utils::Stream::ResponseStream(body); }
    

    
    inline const RequestCharged& GetRequestCharged() const{ return m_requestCharged; }

    
    inline void SetRequestCharged(const RequestCharged& value) { m_requestCharged = value; }

    
    inline void SetRequestCharged(RequestCharged&& value) { m_requestCharged = std::move(value); }

    
    inline GetObjectTorrentResult& WithRequestCharged(const RequestCharged& value) { SetRequestCharged(value); return *this;}

    
    inline GetObjectTorrentResult& WithRequestCharged(RequestCharged&& value) { SetRequestCharged(std::move(value)); return *this;}

  private:

  Aws::Utils::Stream::ResponseStream m_body;

    RequestCharged m_requestCharged;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
