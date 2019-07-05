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
#include <aws/s3/model/StorageClass.h>
#include <aws/s3/model/AccessControlTranslation.h>
#include <aws/s3/model/EncryptionConfiguration.h>
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
   * Container for replication destination information.<p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/Destination">AWS API
   * Reference</a></p>
   */
  class AWS_S3_API Destination
  {
  public:
    Destination();
    Destination(const Aws::Utils::Xml::XmlNode& xmlNode);
    Destination& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * Amazon resource name (ARN) of the bucket where you want Amazon S3 to store
     * replicas of the object identified by the rule.
     */
    inline const Aws::String& GetBucket() const{ return m_bucket; }

    /**
     * Amazon resource name (ARN) of the bucket where you want Amazon S3 to store
     * replicas of the object identified by the rule.
     */
    inline void SetBucket(const Aws::String& value) { m_bucketHasBeenSet = true; m_bucket = value; }

    /**
     * Amazon resource name (ARN) of the bucket where you want Amazon S3 to store
     * replicas of the object identified by the rule.
     */
    inline void SetBucket(Aws::String&& value) { m_bucketHasBeenSet = true; m_bucket = std::move(value); }

    /**
     * Amazon resource name (ARN) of the bucket where you want Amazon S3 to store
     * replicas of the object identified by the rule.
     */
    inline void SetBucket(const char* value) { m_bucketHasBeenSet = true; m_bucket.assign(value); }

    /**
     * Amazon resource name (ARN) of the bucket where you want Amazon S3 to store
     * replicas of the object identified by the rule.
     */
    inline Destination& WithBucket(const Aws::String& value) { SetBucket(value); return *this;}

    /**
     * Amazon resource name (ARN) of the bucket where you want Amazon S3 to store
     * replicas of the object identified by the rule.
     */
    inline Destination& WithBucket(Aws::String&& value) { SetBucket(std::move(value)); return *this;}

    /**
     * Amazon resource name (ARN) of the bucket where you want Amazon S3 to store
     * replicas of the object identified by the rule.
     */
    inline Destination& WithBucket(const char* value) { SetBucket(value); return *this;}


    /**
     * Account ID of the destination bucket. Currently this is only being verified if
     * Access Control Translation is enabled
     */
    inline const Aws::String& GetAccount() const{ return m_account; }

    /**
     * Account ID of the destination bucket. Currently this is only being verified if
     * Access Control Translation is enabled
     */
    inline void SetAccount(const Aws::String& value) { m_accountHasBeenSet = true; m_account = value; }

    /**
     * Account ID of the destination bucket. Currently this is only being verified if
     * Access Control Translation is enabled
     */
    inline void SetAccount(Aws::String&& value) { m_accountHasBeenSet = true; m_account = std::move(value); }

    /**
     * Account ID of the destination bucket. Currently this is only being verified if
     * Access Control Translation is enabled
     */
    inline void SetAccount(const char* value) { m_accountHasBeenSet = true; m_account.assign(value); }

    /**
     * Account ID of the destination bucket. Currently this is only being verified if
     * Access Control Translation is enabled
     */
    inline Destination& WithAccount(const Aws::String& value) { SetAccount(value); return *this;}

    /**
     * Account ID of the destination bucket. Currently this is only being verified if
     * Access Control Translation is enabled
     */
    inline Destination& WithAccount(Aws::String&& value) { SetAccount(std::move(value)); return *this;}

    /**
     * Account ID of the destination bucket. Currently this is only being verified if
     * Access Control Translation is enabled
     */
    inline Destination& WithAccount(const char* value) { SetAccount(value); return *this;}


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
    inline Destination& WithStorageClass(const StorageClass& value) { SetStorageClass(value); return *this;}

    /**
     * The class of storage used to store the object.
     */
    inline Destination& WithStorageClass(StorageClass&& value) { SetStorageClass(std::move(value)); return *this;}


    /**
     * Container for information regarding the access control for replicas.
     */
    inline const AccessControlTranslation& GetAccessControlTranslation() const{ return m_accessControlTranslation; }

    /**
     * Container for information regarding the access control for replicas.
     */
    inline void SetAccessControlTranslation(const AccessControlTranslation& value) { m_accessControlTranslationHasBeenSet = true; m_accessControlTranslation = value; }

    /**
     * Container for information regarding the access control for replicas.
     */
    inline void SetAccessControlTranslation(AccessControlTranslation&& value) { m_accessControlTranslationHasBeenSet = true; m_accessControlTranslation = std::move(value); }

    /**
     * Container for information regarding the access control for replicas.
     */
    inline Destination& WithAccessControlTranslation(const AccessControlTranslation& value) { SetAccessControlTranslation(value); return *this;}

    /**
     * Container for information regarding the access control for replicas.
     */
    inline Destination& WithAccessControlTranslation(AccessControlTranslation&& value) { SetAccessControlTranslation(std::move(value)); return *this;}


    /**
     * Container for information regarding encryption based configuration for replicas.
     */
    inline const EncryptionConfiguration& GetEncryptionConfiguration() const{ return m_encryptionConfiguration; }

    /**
     * Container for information regarding encryption based configuration for replicas.
     */
    inline void SetEncryptionConfiguration(const EncryptionConfiguration& value) { m_encryptionConfigurationHasBeenSet = true; m_encryptionConfiguration = value; }

    /**
     * Container for information regarding encryption based configuration for replicas.
     */
    inline void SetEncryptionConfiguration(EncryptionConfiguration&& value) { m_encryptionConfigurationHasBeenSet = true; m_encryptionConfiguration = std::move(value); }

    /**
     * Container for information regarding encryption based configuration for replicas.
     */
    inline Destination& WithEncryptionConfiguration(const EncryptionConfiguration& value) { SetEncryptionConfiguration(value); return *this;}

    /**
     * Container for information regarding encryption based configuration for replicas.
     */
    inline Destination& WithEncryptionConfiguration(EncryptionConfiguration&& value) { SetEncryptionConfiguration(std::move(value)); return *this;}

  private:

    Aws::String m_bucket;
    bool m_bucketHasBeenSet;

    Aws::String m_account;
    bool m_accountHasBeenSet;

    StorageClass m_storageClass;
    bool m_storageClassHasBeenSet;

    AccessControlTranslation m_accessControlTranslation;
    bool m_accessControlTranslationHasBeenSet;

    EncryptionConfiguration m_encryptionConfiguration;
    bool m_encryptionConfigurationHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
