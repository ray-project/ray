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
#include <aws/s3/model/ServerSideEncryption.h>
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

  /**
   * Describes the default server-side encryption to apply to new objects in the
   * bucket. If Put Object request does not specify any server-side encryption, this
   * default encryption will be applied.<p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/ServerSideEncryptionByDefault">AWS
   * API Reference</a></p>
   */
  class AWS_S3_API ServerSideEncryptionByDefault
  {
  public:
    ServerSideEncryptionByDefault();
    ServerSideEncryptionByDefault(const Aws::Utils::Xml::XmlNode& xmlNode);
    ServerSideEncryptionByDefault& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * Server-side encryption algorithm to use for the default encryption.
     */
    inline const ServerSideEncryption& GetSSEAlgorithm() const{ return m_sSEAlgorithm; }

    /**
     * Server-side encryption algorithm to use for the default encryption.
     */
    inline void SetSSEAlgorithm(const ServerSideEncryption& value) { m_sSEAlgorithmHasBeenSet = true; m_sSEAlgorithm = value; }

    /**
     * Server-side encryption algorithm to use for the default encryption.
     */
    inline void SetSSEAlgorithm(ServerSideEncryption&& value) { m_sSEAlgorithmHasBeenSet = true; m_sSEAlgorithm = std::move(value); }

    /**
     * Server-side encryption algorithm to use for the default encryption.
     */
    inline ServerSideEncryptionByDefault& WithSSEAlgorithm(const ServerSideEncryption& value) { SetSSEAlgorithm(value); return *this;}

    /**
     * Server-side encryption algorithm to use for the default encryption.
     */
    inline ServerSideEncryptionByDefault& WithSSEAlgorithm(ServerSideEncryption&& value) { SetSSEAlgorithm(std::move(value)); return *this;}


    /**
     * KMS master key ID to use for the default encryption. This parameter is allowed
     * if SSEAlgorithm is aws:kms.
     */
    inline const Aws::String& GetKMSMasterKeyID() const{ return m_kMSMasterKeyID; }

    /**
     * KMS master key ID to use for the default encryption. This parameter is allowed
     * if SSEAlgorithm is aws:kms.
     */
    inline void SetKMSMasterKeyID(const Aws::String& value) { m_kMSMasterKeyIDHasBeenSet = true; m_kMSMasterKeyID = value; }

    /**
     * KMS master key ID to use for the default encryption. This parameter is allowed
     * if SSEAlgorithm is aws:kms.
     */
    inline void SetKMSMasterKeyID(Aws::String&& value) { m_kMSMasterKeyIDHasBeenSet = true; m_kMSMasterKeyID = std::move(value); }

    /**
     * KMS master key ID to use for the default encryption. This parameter is allowed
     * if SSEAlgorithm is aws:kms.
     */
    inline void SetKMSMasterKeyID(const char* value) { m_kMSMasterKeyIDHasBeenSet = true; m_kMSMasterKeyID.assign(value); }

    /**
     * KMS master key ID to use for the default encryption. This parameter is allowed
     * if SSEAlgorithm is aws:kms.
     */
    inline ServerSideEncryptionByDefault& WithKMSMasterKeyID(const Aws::String& value) { SetKMSMasterKeyID(value); return *this;}

    /**
     * KMS master key ID to use for the default encryption. This parameter is allowed
     * if SSEAlgorithm is aws:kms.
     */
    inline ServerSideEncryptionByDefault& WithKMSMasterKeyID(Aws::String&& value) { SetKMSMasterKeyID(std::move(value)); return *this;}

    /**
     * KMS master key ID to use for the default encryption. This parameter is allowed
     * if SSEAlgorithm is aws:kms.
     */
    inline ServerSideEncryptionByDefault& WithKMSMasterKeyID(const char* value) { SetKMSMasterKeyID(value); return *this;}

  private:

    ServerSideEncryption m_sSEAlgorithm;
    bool m_sSEAlgorithmHasBeenSet;

    Aws::String m_kMSMasterKeyID;
    bool m_kMSMasterKeyIDHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
