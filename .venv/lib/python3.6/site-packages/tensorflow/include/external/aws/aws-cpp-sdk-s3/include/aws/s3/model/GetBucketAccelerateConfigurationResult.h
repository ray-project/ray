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
#include <aws/s3/model/BucketAccelerateStatus.h>
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
  class AWS_S3_API GetBucketAccelerateConfigurationResult
  {
  public:
    GetBucketAccelerateConfigurationResult();
    GetBucketAccelerateConfigurationResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    GetBucketAccelerateConfigurationResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * The accelerate configuration of the bucket.
     */
    inline const BucketAccelerateStatus& GetStatus() const{ return m_status; }

    /**
     * The accelerate configuration of the bucket.
     */
    inline void SetStatus(const BucketAccelerateStatus& value) { m_status = value; }

    /**
     * The accelerate configuration of the bucket.
     */
    inline void SetStatus(BucketAccelerateStatus&& value) { m_status = std::move(value); }

    /**
     * The accelerate configuration of the bucket.
     */
    inline GetBucketAccelerateConfigurationResult& WithStatus(const BucketAccelerateStatus& value) { SetStatus(value); return *this;}

    /**
     * The accelerate configuration of the bucket.
     */
    inline GetBucketAccelerateConfigurationResult& WithStatus(BucketAccelerateStatus&& value) { SetStatus(std::move(value)); return *this;}

  private:

    BucketAccelerateStatus m_status;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
