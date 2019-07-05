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
#include <aws/core/utils/DateTime.h>
#include <aws/s3/model/ObjectStorageClass.h>
#include <aws/s3/model/Owner.h>
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

  class AWS_S3_API Object
  {
  public:
    Object();
    Object(const Aws::Utils::Xml::XmlNode& xmlNode);
    Object& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    
    inline const Aws::String& GetKey() const{ return m_key; }

    
    inline void SetKey(const Aws::String& value) { m_keyHasBeenSet = true; m_key = value; }

    
    inline void SetKey(Aws::String&& value) { m_keyHasBeenSet = true; m_key = std::move(value); }

    
    inline void SetKey(const char* value) { m_keyHasBeenSet = true; m_key.assign(value); }

    
    inline Object& WithKey(const Aws::String& value) { SetKey(value); return *this;}

    
    inline Object& WithKey(Aws::String&& value) { SetKey(std::move(value)); return *this;}

    
    inline Object& WithKey(const char* value) { SetKey(value); return *this;}


    
    inline const Aws::Utils::DateTime& GetLastModified() const{ return m_lastModified; }

    
    inline void SetLastModified(const Aws::Utils::DateTime& value) { m_lastModifiedHasBeenSet = true; m_lastModified = value; }

    
    inline void SetLastModified(Aws::Utils::DateTime&& value) { m_lastModifiedHasBeenSet = true; m_lastModified = std::move(value); }

    
    inline Object& WithLastModified(const Aws::Utils::DateTime& value) { SetLastModified(value); return *this;}

    
    inline Object& WithLastModified(Aws::Utils::DateTime&& value) { SetLastModified(std::move(value)); return *this;}


    
    inline const Aws::String& GetETag() const{ return m_eTag; }

    
    inline void SetETag(const Aws::String& value) { m_eTagHasBeenSet = true; m_eTag = value; }

    
    inline void SetETag(Aws::String&& value) { m_eTagHasBeenSet = true; m_eTag = std::move(value); }

    
    inline void SetETag(const char* value) { m_eTagHasBeenSet = true; m_eTag.assign(value); }

    
    inline Object& WithETag(const Aws::String& value) { SetETag(value); return *this;}

    
    inline Object& WithETag(Aws::String&& value) { SetETag(std::move(value)); return *this;}

    
    inline Object& WithETag(const char* value) { SetETag(value); return *this;}


    
    inline long long GetSize() const{ return m_size; }

    
    inline void SetSize(long long value) { m_sizeHasBeenSet = true; m_size = value; }

    
    inline Object& WithSize(long long value) { SetSize(value); return *this;}


    /**
     * The class of storage used to store the object.
     */
    inline const ObjectStorageClass& GetStorageClass() const{ return m_storageClass; }

    /**
     * The class of storage used to store the object.
     */
    inline void SetStorageClass(const ObjectStorageClass& value) { m_storageClassHasBeenSet = true; m_storageClass = value; }

    /**
     * The class of storage used to store the object.
     */
    inline void SetStorageClass(ObjectStorageClass&& value) { m_storageClassHasBeenSet = true; m_storageClass = std::move(value); }

    /**
     * The class of storage used to store the object.
     */
    inline Object& WithStorageClass(const ObjectStorageClass& value) { SetStorageClass(value); return *this;}

    /**
     * The class of storage used to store the object.
     */
    inline Object& WithStorageClass(ObjectStorageClass&& value) { SetStorageClass(std::move(value)); return *this;}


    
    inline const Owner& GetOwner() const{ return m_owner; }

    
    inline void SetOwner(const Owner& value) { m_ownerHasBeenSet = true; m_owner = value; }

    
    inline void SetOwner(Owner&& value) { m_ownerHasBeenSet = true; m_owner = std::move(value); }

    
    inline Object& WithOwner(const Owner& value) { SetOwner(value); return *this;}

    
    inline Object& WithOwner(Owner&& value) { SetOwner(std::move(value)); return *this;}

  private:

    Aws::String m_key;
    bool m_keyHasBeenSet;

    Aws::Utils::DateTime m_lastModified;
    bool m_lastModifiedHasBeenSet;

    Aws::String m_eTag;
    bool m_eTagHasBeenSet;

    long long m_size;
    bool m_sizeHasBeenSet;

    ObjectStorageClass m_storageClass;
    bool m_storageClassHasBeenSet;

    Owner m_owner;
    bool m_ownerHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
