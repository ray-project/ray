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
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/DateTime.h>
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

  class AWS_S3_API DeleteMarkerEntry
  {
  public:
    DeleteMarkerEntry();
    DeleteMarkerEntry(const Aws::Utils::Xml::XmlNode& xmlNode);
    DeleteMarkerEntry& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    
    inline const Owner& GetOwner() const{ return m_owner; }

    
    inline void SetOwner(const Owner& value) { m_ownerHasBeenSet = true; m_owner = value; }

    
    inline void SetOwner(Owner&& value) { m_ownerHasBeenSet = true; m_owner = std::move(value); }

    
    inline DeleteMarkerEntry& WithOwner(const Owner& value) { SetOwner(value); return *this;}

    
    inline DeleteMarkerEntry& WithOwner(Owner&& value) { SetOwner(std::move(value)); return *this;}


    /**
     * The object key.
     */
    inline const Aws::String& GetKey() const{ return m_key; }

    /**
     * The object key.
     */
    inline void SetKey(const Aws::String& value) { m_keyHasBeenSet = true; m_key = value; }

    /**
     * The object key.
     */
    inline void SetKey(Aws::String&& value) { m_keyHasBeenSet = true; m_key = std::move(value); }

    /**
     * The object key.
     */
    inline void SetKey(const char* value) { m_keyHasBeenSet = true; m_key.assign(value); }

    /**
     * The object key.
     */
    inline DeleteMarkerEntry& WithKey(const Aws::String& value) { SetKey(value); return *this;}

    /**
     * The object key.
     */
    inline DeleteMarkerEntry& WithKey(Aws::String&& value) { SetKey(std::move(value)); return *this;}

    /**
     * The object key.
     */
    inline DeleteMarkerEntry& WithKey(const char* value) { SetKey(value); return *this;}


    /**
     * Version ID of an object.
     */
    inline const Aws::String& GetVersionId() const{ return m_versionId; }

    /**
     * Version ID of an object.
     */
    inline void SetVersionId(const Aws::String& value) { m_versionIdHasBeenSet = true; m_versionId = value; }

    /**
     * Version ID of an object.
     */
    inline void SetVersionId(Aws::String&& value) { m_versionIdHasBeenSet = true; m_versionId = std::move(value); }

    /**
     * Version ID of an object.
     */
    inline void SetVersionId(const char* value) { m_versionIdHasBeenSet = true; m_versionId.assign(value); }

    /**
     * Version ID of an object.
     */
    inline DeleteMarkerEntry& WithVersionId(const Aws::String& value) { SetVersionId(value); return *this;}

    /**
     * Version ID of an object.
     */
    inline DeleteMarkerEntry& WithVersionId(Aws::String&& value) { SetVersionId(std::move(value)); return *this;}

    /**
     * Version ID of an object.
     */
    inline DeleteMarkerEntry& WithVersionId(const char* value) { SetVersionId(value); return *this;}


    /**
     * Specifies whether the object is (true) or is not (false) the latest version of
     * an object.
     */
    inline bool GetIsLatest() const{ return m_isLatest; }

    /**
     * Specifies whether the object is (true) or is not (false) the latest version of
     * an object.
     */
    inline void SetIsLatest(bool value) { m_isLatestHasBeenSet = true; m_isLatest = value; }

    /**
     * Specifies whether the object is (true) or is not (false) the latest version of
     * an object.
     */
    inline DeleteMarkerEntry& WithIsLatest(bool value) { SetIsLatest(value); return *this;}


    /**
     * Date and time the object was last modified.
     */
    inline const Aws::Utils::DateTime& GetLastModified() const{ return m_lastModified; }

    /**
     * Date and time the object was last modified.
     */
    inline void SetLastModified(const Aws::Utils::DateTime& value) { m_lastModifiedHasBeenSet = true; m_lastModified = value; }

    /**
     * Date and time the object was last modified.
     */
    inline void SetLastModified(Aws::Utils::DateTime&& value) { m_lastModifiedHasBeenSet = true; m_lastModified = std::move(value); }

    /**
     * Date and time the object was last modified.
     */
    inline DeleteMarkerEntry& WithLastModified(const Aws::Utils::DateTime& value) { SetLastModified(value); return *this;}

    /**
     * Date and time the object was last modified.
     */
    inline DeleteMarkerEntry& WithLastModified(Aws::Utils::DateTime&& value) { SetLastModified(std::move(value)); return *this;}

  private:

    Owner m_owner;
    bool m_ownerHasBeenSet;

    Aws::String m_key;
    bool m_keyHasBeenSet;

    Aws::String m_versionId;
    bool m_versionIdHasBeenSet;

    bool m_isLatest;
    bool m_isLatestHasBeenSet;

    Aws::Utils::DateTime m_lastModified;
    bool m_lastModifiedHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
