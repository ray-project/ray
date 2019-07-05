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
#include <aws/s3/model/Owner.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/s3/model/RequestCharged.h>
#include <aws/s3/model/Grant.h>
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
  class AWS_S3_API GetObjectAclResult
  {
  public:
    GetObjectAclResult();
    GetObjectAclResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    GetObjectAclResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    
    inline const Owner& GetOwner() const{ return m_owner; }

    
    inline void SetOwner(const Owner& value) { m_owner = value; }

    
    inline void SetOwner(Owner&& value) { m_owner = std::move(value); }

    
    inline GetObjectAclResult& WithOwner(const Owner& value) { SetOwner(value); return *this;}

    
    inline GetObjectAclResult& WithOwner(Owner&& value) { SetOwner(std::move(value)); return *this;}


    /**
     * A list of grants.
     */
    inline const Aws::Vector<Grant>& GetGrants() const{ return m_grants; }

    /**
     * A list of grants.
     */
    inline void SetGrants(const Aws::Vector<Grant>& value) { m_grants = value; }

    /**
     * A list of grants.
     */
    inline void SetGrants(Aws::Vector<Grant>&& value) { m_grants = std::move(value); }

    /**
     * A list of grants.
     */
    inline GetObjectAclResult& WithGrants(const Aws::Vector<Grant>& value) { SetGrants(value); return *this;}

    /**
     * A list of grants.
     */
    inline GetObjectAclResult& WithGrants(Aws::Vector<Grant>&& value) { SetGrants(std::move(value)); return *this;}

    /**
     * A list of grants.
     */
    inline GetObjectAclResult& AddGrants(const Grant& value) { m_grants.push_back(value); return *this; }

    /**
     * A list of grants.
     */
    inline GetObjectAclResult& AddGrants(Grant&& value) { m_grants.push_back(std::move(value)); return *this; }


    
    inline const RequestCharged& GetRequestCharged() const{ return m_requestCharged; }

    
    inline void SetRequestCharged(const RequestCharged& value) { m_requestCharged = value; }

    
    inline void SetRequestCharged(RequestCharged&& value) { m_requestCharged = std::move(value); }

    
    inline GetObjectAclResult& WithRequestCharged(const RequestCharged& value) { SetRequestCharged(value); return *this;}

    
    inline GetObjectAclResult& WithRequestCharged(RequestCharged&& value) { SetRequestCharged(std::move(value)); return *this;}

  private:

    Owner m_owner;

    Aws::Vector<Grant> m_grants;

    RequestCharged m_requestCharged;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
