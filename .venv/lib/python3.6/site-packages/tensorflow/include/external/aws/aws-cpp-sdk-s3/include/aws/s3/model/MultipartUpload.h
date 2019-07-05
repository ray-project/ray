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
#include <aws/s3/model/StorageClass.h>
#include <aws/s3/model/Owner.h>
#include <aws/s3/model/Initiator.h>
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

  class AWS_S3_API MultipartUpload
  {
  public:
    MultipartUpload();
    MultipartUpload(const Aws::Utils::Xml::XmlNode& xmlNode);
    MultipartUpload& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * Upload ID that identifies the multipart upload.
     */
    inline const Aws::String& GetUploadId() const{ return m_uploadId; }

    /**
     * Upload ID that identifies the multipart upload.
     */
    inline void SetUploadId(const Aws::String& value) { m_uploadIdHasBeenSet = true; m_uploadId = value; }

    /**
     * Upload ID that identifies the multipart upload.
     */
    inline void SetUploadId(Aws::String&& value) { m_uploadIdHasBeenSet = true; m_uploadId = std::move(value); }

    /**
     * Upload ID that identifies the multipart upload.
     */
    inline void SetUploadId(const char* value) { m_uploadIdHasBeenSet = true; m_uploadId.assign(value); }

    /**
     * Upload ID that identifies the multipart upload.
     */
    inline MultipartUpload& WithUploadId(const Aws::String& value) { SetUploadId(value); return *this;}

    /**
     * Upload ID that identifies the multipart upload.
     */
    inline MultipartUpload& WithUploadId(Aws::String&& value) { SetUploadId(std::move(value)); return *this;}

    /**
     * Upload ID that identifies the multipart upload.
     */
    inline MultipartUpload& WithUploadId(const char* value) { SetUploadId(value); return *this;}


    /**
     * Key of the object for which the multipart upload was initiated.
     */
    inline const Aws::String& GetKey() const{ return m_key; }

    /**
     * Key of the object for which the multipart upload was initiated.
     */
    inline void SetKey(const Aws::String& value) { m_keyHasBeenSet = true; m_key = value; }

    /**
     * Key of the object for which the multipart upload was initiated.
     */
    inline void SetKey(Aws::String&& value) { m_keyHasBeenSet = true; m_key = std::move(value); }

    /**
     * Key of the object for which the multipart upload was initiated.
     */
    inline void SetKey(const char* value) { m_keyHasBeenSet = true; m_key.assign(value); }

    /**
     * Key of the object for which the multipart upload was initiated.
     */
    inline MultipartUpload& WithKey(const Aws::String& value) { SetKey(value); return *this;}

    /**
     * Key of the object for which the multipart upload was initiated.
     */
    inline MultipartUpload& WithKey(Aws::String&& value) { SetKey(std::move(value)); return *this;}

    /**
     * Key of the object for which the multipart upload was initiated.
     */
    inline MultipartUpload& WithKey(const char* value) { SetKey(value); return *this;}


    /**
     * Date and time at which the multipart upload was initiated.
     */
    inline const Aws::Utils::DateTime& GetInitiated() const{ return m_initiated; }

    /**
     * Date and time at which the multipart upload was initiated.
     */
    inline void SetInitiated(const Aws::Utils::DateTime& value) { m_initiatedHasBeenSet = true; m_initiated = value; }

    /**
     * Date and time at which the multipart upload was initiated.
     */
    inline void SetInitiated(Aws::Utils::DateTime&& value) { m_initiatedHasBeenSet = true; m_initiated = std::move(value); }

    /**
     * Date and time at which the multipart upload was initiated.
     */
    inline MultipartUpload& WithInitiated(const Aws::Utils::DateTime& value) { SetInitiated(value); return *this;}

    /**
     * Date and time at which the multipart upload was initiated.
     */
    inline MultipartUpload& WithInitiated(Aws::Utils::DateTime&& value) { SetInitiated(std::move(value)); return *this;}


    /**
     * The class of storage used to store the object.
     */
    inline const StorageClass& GetStorageClass() const{ return m_storageClass; }

    /**
     * The class of storage used to store the object.
     */
    inline void SetStorageClass(const StorageClass& value) { m_storageClassHasBeenSet = true; m_storageClass = value; }

    /**
     * The class of storage used to store the object.
     */
    inline void SetStorageClass(StorageClass&& value) { m_storageClassHasBeenSet = true; m_storageClass = std::move(value); }

    /**
     * The class of storage used to store the object.
     */
    inline MultipartUpload& WithStorageClass(const StorageClass& value) { SetStorageClass(value); return *this;}

    /**
     * The class of storage used to store the object.
     */
    inline MultipartUpload& WithStorageClass(StorageClass&& value) { SetStorageClass(std::move(value)); return *this;}


    
    inline const Owner& GetOwner() const{ return m_owner; }

    
    inline void SetOwner(const Owner& value) { m_ownerHasBeenSet = true; m_owner = value; }

    
    inline void SetOwner(Owner&& value) { m_ownerHasBeenSet = true; m_owner = std::move(value); }

    
    inline MultipartUpload& WithOwner(const Owner& value) { SetOwner(value); return *this;}

    
    inline MultipartUpload& WithOwner(Owner&& value) { SetOwner(std::move(value)); return *this;}


    /**
     * Identifies who initiated the multipart upload.
     */
    inline const Initiator& GetInitiator() const{ return m_initiator; }

    /**
     * Identifies who initiated the multipart upload.
     */
    inline void SetInitiator(const Initiator& value) { m_initiatorHasBeenSet = true; m_initiator = value; }

    /**
     * Identifies who initiated the multipart upload.
     */
    inline void SetInitiator(Initiator&& value) { m_initiatorHasBeenSet = true; m_initiator = std::move(value); }

    /**
     * Identifies who initiated the multipart upload.
     */
    inline MultipartUpload& WithInitiator(const Initiator& value) { SetInitiator(value); return *this;}

    /**
     * Identifies who initiated the multipart upload.
     */
    inline MultipartUpload& WithInitiator(Initiator&& value) { SetInitiator(std::move(value)); return *this;}

  private:

    Aws::String m_uploadId;
    bool m_uploadIdHasBeenSet;

    Aws::String m_key;
    bool m_keyHasBeenSet;

    Aws::Utils::DateTime m_initiated;
    bool m_initiatedHasBeenSet;

    StorageClass m_storageClass;
    bool m_storageClassHasBeenSet;

    Owner m_owner;
    bool m_ownerHasBeenSet;

    Initiator m_initiator;
    bool m_initiatorHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
