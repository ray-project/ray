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
#include <aws/s3/model/RequestCharged.h>
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
  class AWS_S3_API DeleteObjectResult
  {
  public:
    DeleteObjectResult();
    DeleteObjectResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    DeleteObjectResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * Specifies whether the versioned object that was permanently deleted was (true)
     * or was not (false) a delete marker.
     */
    inline bool GetDeleteMarker() const{ return m_deleteMarker; }

    /**
     * Specifies whether the versioned object that was permanently deleted was (true)
     * or was not (false) a delete marker.
     */
    inline void SetDeleteMarker(bool value) { m_deleteMarker = value; }

    /**
     * Specifies whether the versioned object that was permanently deleted was (true)
     * or was not (false) a delete marker.
     */
    inline DeleteObjectResult& WithDeleteMarker(bool value) { SetDeleteMarker(value); return *this;}


    /**
     * Returns the version ID of the delete marker created as a result of the DELETE
     * operation.
     */
    inline const Aws::String& GetVersionId() const{ return m_versionId; }

    /**
     * Returns the version ID of the delete marker created as a result of the DELETE
     * operation.
     */
    inline void SetVersionId(const Aws::String& value) { m_versionId = value; }

    /**
     * Returns the version ID of the delete marker created as a result of the DELETE
     * operation.
     */
    inline void SetVersionId(Aws::String&& value) { m_versionId = std::move(value); }

    /**
     * Returns the version ID of the delete marker created as a result of the DELETE
     * operation.
     */
    inline void SetVersionId(const char* value) { m_versionId.assign(value); }

    /**
     * Returns the version ID of the delete marker created as a result of the DELETE
     * operation.
     */
    inline DeleteObjectResult& WithVersionId(const Aws::String& value) { SetVersionId(value); return *this;}

    /**
     * Returns the version ID of the delete marker created as a result of the DELETE
     * operation.
     */
    inline DeleteObjectResult& WithVersionId(Aws::String&& value) { SetVersionId(std::move(value)); return *this;}

    /**
     * Returns the version ID of the delete marker created as a result of the DELETE
     * operation.
     */
    inline DeleteObjectResult& WithVersionId(const char* value) { SetVersionId(value); return *this;}


    
    inline const RequestCharged& GetRequestCharged() const{ return m_requestCharged; }

    
    inline void SetRequestCharged(const RequestCharged& value) { m_requestCharged = value; }

    
    inline void SetRequestCharged(RequestCharged&& value) { m_requestCharged = std::move(value); }

    
    inline DeleteObjectResult& WithRequestCharged(const RequestCharged& value) { SetRequestCharged(value); return *this;}

    
    inline DeleteObjectResult& WithRequestCharged(RequestCharged&& value) { SetRequestCharged(std::move(value)); return *this;}

  private:

    bool m_deleteMarker;

    Aws::String m_versionId;

    RequestCharged m_requestCharged;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
