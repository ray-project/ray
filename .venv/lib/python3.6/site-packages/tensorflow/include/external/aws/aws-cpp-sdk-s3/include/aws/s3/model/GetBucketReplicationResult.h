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
#include <aws/s3/model/ReplicationConfiguration.h>
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
  class AWS_S3_API GetBucketReplicationResult
  {
  public:
    GetBucketReplicationResult();
    GetBucketReplicationResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    GetBucketReplicationResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    
    inline const ReplicationConfiguration& GetReplicationConfiguration() const{ return m_replicationConfiguration; }

    
    inline void SetReplicationConfiguration(const ReplicationConfiguration& value) { m_replicationConfiguration = value; }

    
    inline void SetReplicationConfiguration(ReplicationConfiguration&& value) { m_replicationConfiguration = std::move(value); }

    
    inline GetBucketReplicationResult& WithReplicationConfiguration(const ReplicationConfiguration& value) { SetReplicationConfiguration(value); return *this;}

    
    inline GetBucketReplicationResult& WithReplicationConfiguration(ReplicationConfiguration&& value) { SetReplicationConfiguration(std::move(value)); return *this;}

  private:

    ReplicationConfiguration m_replicationConfiguration;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
