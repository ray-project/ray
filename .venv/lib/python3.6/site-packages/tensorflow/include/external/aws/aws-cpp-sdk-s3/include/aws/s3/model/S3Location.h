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
#include <aws/s3/model/Encryption.h>
#include <aws/s3/model/ObjectCannedACL.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/s3/model/Tagging.h>
#include <aws/s3/model/StorageClass.h>
#include <aws/s3/model/Grant.h>
#include <aws/s3/model/MetadataEntry.h>
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

  /**
   * Describes an S3 location that will receive the results of the restore
   * request.<p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/S3Location">AWS API
   * Reference</a></p>
   */
  class AWS_S3_API S3Location
  {
  public:
    S3Location();
    S3Location(const Aws::Utils::Xml::XmlNode& xmlNode);
    S3Location& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * The name of the bucket where the restore results will be placed.
     */
    inline const Aws::String& GetBucketName() const{ return m_bucketName; }

    /**
     * The name of the bucket where the restore results will be placed.
     */
    inline void SetBucketName(const Aws::String& value) { m_bucketNameHasBeenSet = true; m_bucketName = value; }

    /**
     * The name of the bucket where the restore results will be placed.
     */
    inline void SetBucketName(Aws::String&& value) { m_bucketNameHasBeenSet = true; m_bucketName = std::move(value); }

    /**
     * The name of the bucket where the restore results will be placed.
     */
    inline void SetBucketName(const char* value) { m_bucketNameHasBeenSet = true; m_bucketName.assign(value); }

    /**
     * The name of the bucket where the restore results will be placed.
     */
    inline S3Location& WithBucketName(const Aws::String& value) { SetBucketName(value); return *this;}

    /**
     * The name of the bucket where the restore results will be placed.
     */
    inline S3Location& WithBucketName(Aws::String&& value) { SetBucketName(std::move(value)); return *this;}

    /**
     * The name of the bucket where the restore results will be placed.
     */
    inline S3Location& WithBucketName(const char* value) { SetBucketName(value); return *this;}


    /**
     * The prefix that is prepended to the restore results for this request.
     */
    inline const Aws::String& GetPrefix() const{ return m_prefix; }

    /**
     * The prefix that is prepended to the restore results for this request.
     */
    inline void SetPrefix(const Aws::String& value) { m_prefixHasBeenSet = true; m_prefix = value; }

    /**
     * The prefix that is prepended to the restore results for this request.
     */
    inline void SetPrefix(Aws::String&& value) { m_prefixHasBeenSet = true; m_prefix = std::move(value); }

    /**
     * The prefix that is prepended to the restore results for this request.
     */
    inline void SetPrefix(const char* value) { m_prefixHasBeenSet = true; m_prefix.assign(value); }

    /**
     * The prefix that is prepended to the restore results for this request.
     */
    inline S3Location& WithPrefix(const Aws::String& value) { SetPrefix(value); return *this;}

    /**
     * The prefix that is prepended to the restore results for this request.
     */
    inline S3Location& WithPrefix(Aws::String&& value) { SetPrefix(std::move(value)); return *this;}

    /**
     * The prefix that is prepended to the restore results for this request.
     */
    inline S3Location& WithPrefix(const char* value) { SetPrefix(value); return *this;}


    
    inline const Encryption& GetEncryption() const{ return m_encryption; }

    
    inline void SetEncryption(const Encryption& value) { m_encryptionHasBeenSet = true; m_encryption = value; }

    
    inline void SetEncryption(Encryption&& value) { m_encryptionHasBeenSet = true; m_encryption = std::move(value); }

    
    inline S3Location& WithEncryption(const Encryption& value) { SetEncryption(value); return *this;}

    
    inline S3Location& WithEncryption(Encryption&& value) { SetEncryption(std::move(value)); return *this;}


    /**
     * The canned ACL to apply to the restore results.
     */
    inline const ObjectCannedACL& GetCannedACL() const{ return m_cannedACL; }

    /**
     * The canned ACL to apply to the restore results.
     */
    inline void SetCannedACL(const ObjectCannedACL& value) { m_cannedACLHasBeenSet = true; m_cannedACL = value; }

    /**
     * The canned ACL to apply to the restore results.
     */
    inline void SetCannedACL(ObjectCannedACL&& value) { m_cannedACLHasBeenSet = true; m_cannedACL = std::move(value); }

    /**
     * The canned ACL to apply to the restore results.
     */
    inline S3Location& WithCannedACL(const ObjectCannedACL& value) { SetCannedACL(value); return *this;}

    /**
     * The canned ACL to apply to the restore results.
     */
    inline S3Location& WithCannedACL(ObjectCannedACL&& value) { SetCannedACL(std::move(value)); return *this;}


    /**
     * A list of grants that control access to the staged results.
     */
    inline const Aws::Vector<Grant>& GetAccessControlList() const{ return m_accessControlList; }

    /**
     * A list of grants that control access to the staged results.
     */
    inline void SetAccessControlList(const Aws::Vector<Grant>& value) { m_accessControlListHasBeenSet = true; m_accessControlList = value; }

    /**
     * A list of grants that control access to the staged results.
     */
    inline void SetAccessControlList(Aws::Vector<Grant>&& value) { m_accessControlListHasBeenSet = true; m_accessControlList = std::move(value); }

    /**
     * A list of grants that control access to the staged results.
     */
    inline S3Location& WithAccessControlList(const Aws::Vector<Grant>& value) { SetAccessControlList(value); return *this;}

    /**
     * A list of grants that control access to the staged results.
     */
    inline S3Location& WithAccessControlList(Aws::Vector<Grant>&& value) { SetAccessControlList(std::move(value)); return *this;}

    /**
     * A list of grants that control access to the staged results.
     */
    inline S3Location& AddAccessControlList(const Grant& value) { m_accessControlListHasBeenSet = true; m_accessControlList.push_back(value); return *this; }

    /**
     * A list of grants that control access to the staged results.
     */
    inline S3Location& AddAccessControlList(Grant&& value) { m_accessControlListHasBeenSet = true; m_accessControlList.push_back(std::move(value)); return *this; }


    /**
     * The tag-set that is applied to the restore results.
     */
    inline const Tagging& GetTagging() const{ return m_tagging; }

    /**
     * The tag-set that is applied to the restore results.
     */
    inline void SetTagging(const Tagging& value) { m_taggingHasBeenSet = true; m_tagging = value; }

    /**
     * The tag-set that is applied to the restore results.
     */
    inline void SetTagging(Tagging&& value) { m_taggingHasBeenSet = true; m_tagging = std::move(value); }

    /**
     * The tag-set that is applied to the restore results.
     */
    inline S3Location& WithTagging(const Tagging& value) { SetTagging(value); return *this;}

    /**
     * The tag-set that is applied to the restore results.
     */
    inline S3Location& WithTagging(Tagging&& value) { SetTagging(std::move(value)); return *this;}


    /**
     * A list of metadata to store with the restore results in S3.
     */
    inline const Aws::Vector<MetadataEntry>& GetUserMetadata() const{ return m_userMetadata; }

    /**
     * A list of metadata to store with the restore results in S3.
     */
    inline void SetUserMetadata(const Aws::Vector<MetadataEntry>& value) { m_userMetadataHasBeenSet = true; m_userMetadata = value; }

    /**
     * A list of metadata to store with the restore results in S3.
     */
    inline void SetUserMetadata(Aws::Vector<MetadataEntry>&& value) { m_userMetadataHasBeenSet = true; m_userMetadata = std::move(value); }

    /**
     * A list of metadata to store with the restore results in S3.
     */
    inline S3Location& WithUserMetadata(const Aws::Vector<MetadataEntry>& value) { SetUserMetadata(value); return *this;}

    /**
     * A list of metadata to store with the restore results in S3.
     */
    inline S3Location& WithUserMetadata(Aws::Vector<MetadataEntry>&& value) { SetUserMetadata(std::move(value)); return *this;}

    /**
     * A list of metadata to store with the restore results in S3.
     */
    inline S3Location& AddUserMetadata(const MetadataEntry& value) { m_userMetadataHasBeenSet = true; m_userMetadata.push_back(value); return *this; }

    /**
     * A list of metadata to store with the restore results in S3.
     */
    inline S3Location& AddUserMetadata(MetadataEntry&& value) { m_userMetadataHasBeenSet = true; m_userMetadata.push_back(std::move(value)); return *this; }


    /**
     * The class of storage used to store the restore results.
     */
    inline const StorageClass& GetStorageClass() const{ return m_storageClass; }

    /**
     * The class of storage used to store the restore results.
     */
    inline void SetStorageClass(const StorageClass& value) { m_storageClassHasBeenSet = true; m_storageClass = value; }

    /**
     * The class of storage used to store the restore results.
     */
    inline void SetStorageClass(StorageClass&& value) { m_storageClassHasBeenSet = true; m_storageClass = std::move(value); }

    /**
     * The class of storage used to store the restore results.
     */
    inline S3Location& WithStorageClass(const StorageClass& value) { SetStorageClass(value); return *this;}

    /**
     * The class of storage used to store the restore results.
     */
    inline S3Location& WithStorageClass(StorageClass&& value) { SetStorageClass(std::move(value)); return *this;}

  private:

    Aws::String m_bucketName;
    bool m_bucketNameHasBeenSet;

    Aws::String m_prefix;
    bool m_prefixHasBeenSet;

    Encryption m_encryption;
    bool m_encryptionHasBeenSet;

    ObjectCannedACL m_cannedACL;
    bool m_cannedACLHasBeenSet;

    Aws::Vector<Grant> m_accessControlList;
    bool m_accessControlListHasBeenSet;

    Tagging m_tagging;
    bool m_taggingHasBeenSet;

    Aws::Vector<MetadataEntry> m_userMetadata;
    bool m_userMetadataHasBeenSet;

    StorageClass m_storageClass;
    bool m_storageClassHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
