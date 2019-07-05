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
#include <utility>

namespace Aws
{
namespace Utils
{
namespace Xml
{
  class XmlNode;
} // namespace Xml
} // namespace Utils
namespace S3
{
namespace Model
{

  class AWS_S3_API DeletedObject
  {
  public:
    DeletedObject();
    DeletedObject(const Aws::Utils::Xml::XmlNode& xmlNode);
    DeletedObject& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    
    inline const Aws::String& GetKey() const{ return m_key; }

    
    inline void SetKey(const Aws::String& value) { m_keyHasBeenSet = true; m_key = value; }

    
    inline void SetKey(Aws::String&& value) { m_keyHasBeenSet = true; m_key = std::move(value); }

    
    inline void SetKey(const char* value) { m_keyHasBeenSet = true; m_key.assign(value); }

    
    inline DeletedObject& WithKey(const Aws::String& value) { SetKey(value); return *this;}

    
    inline DeletedObject& WithKey(Aws::String&& value) { SetKey(std::move(value)); return *this;}

    
    inline DeletedObject& WithKey(const char* value) { SetKey(value); return *this;}


    
    inline const Aws::String& GetVersionId() const{ return m_versionId; }

    
    inline void SetVersionId(const Aws::String& value) { m_versionIdHasBeenSet = true; m_versionId = value; }

    
    inline void SetVersionId(Aws::String&& value) { m_versionIdHasBeenSet = true; m_versionId = std::move(value); }

    
    inline void SetVersionId(const char* value) { m_versionIdHasBeenSet = true; m_versionId.assign(value); }

    
    inline DeletedObject& WithVersionId(const Aws::String& value) { SetVersionId(value); return *this;}

    
    inline DeletedObject& WithVersionId(Aws::String&& value) { SetVersionId(std::move(value)); return *this;}

    
    inline DeletedObject& WithVersionId(const char* value) { SetVersionId(value); return *this;}


    
    inline bool GetDeleteMarker() const{ return m_deleteMarker; }

    
    inline void SetDeleteMarker(bool value) { m_deleteMarkerHasBeenSet = true; m_deleteMarker = value; }

    
    inline DeletedObject& WithDeleteMarker(bool value) { SetDeleteMarker(value); return *this;}


    
    inline const Aws::String& GetDeleteMarkerVersionId() const{ return m_deleteMarkerVersionId; }

    
    inline void SetDeleteMarkerVersionId(const Aws::String& value) { m_deleteMarkerVersionIdHasBeenSet = true; m_deleteMarkerVersionId = value; }

    
    inline void SetDeleteMarkerVersionId(Aws::String&& value) { m_deleteMarkerVersionIdHasBeenSet = true; m_deleteMarkerVersionId = std::move(value); }

    
    inline void SetDeleteMarkerVersionId(const char* value) { m_deleteMarkerVersionIdHasBeenSet = true; m_deleteMarkerVersionId.assign(value); }

    
    inline DeletedObject& WithDeleteMarkerVersionId(const Aws::String& value) { SetDeleteMarkerVersionId(value); return *this;}

    
    inline DeletedObject& WithDeleteMarkerVersionId(Aws::String&& value) { SetDeleteMarkerVersionId(std::move(value)); return *this;}

    
    inline DeletedObject& WithDeleteMarkerVersionId(const char* value) { SetDeleteMarkerVersionId(value); return *this;}

  private:

    Aws::String m_key;
    bool m_keyHasBeenSet;

    Aws::String m_versionId;
    bool m_versionIdHasBeenSet;

    bool m_deleteMarker;
    bool m_deleteMarkerHasBeenSet;

    Aws::String m_deleteMarkerVersionId;
    bool m_deleteMarkerVersionIdHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
