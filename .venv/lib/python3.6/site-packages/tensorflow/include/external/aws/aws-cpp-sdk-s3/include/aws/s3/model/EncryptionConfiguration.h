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

  /**
   * Container for information regarding encryption based configuration for
   * replicas.<p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/EncryptionConfiguration">AWS
   * API Reference</a></p>
   */
  class AWS_S3_API EncryptionConfiguration
  {
  public:
    EncryptionConfiguration();
    EncryptionConfiguration(const Aws::Utils::Xml::XmlNode& xmlNode);
    EncryptionConfiguration& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * The id of the KMS key used to encrypt the replica object.
     */
    inline const Aws::String& GetReplicaKmsKeyID() const{ return m_replicaKmsKeyID; }

    /**
     * The id of the KMS key used to encrypt the replica object.
     */
    inline void SetReplicaKmsKeyID(const Aws::String& value) { m_replicaKmsKeyIDHasBeenSet = true; m_replicaKmsKeyID = value; }

    /**
     * The id of the KMS key used to encrypt the replica object.
     */
    inline void SetReplicaKmsKeyID(Aws::String&& value) { m_replicaKmsKeyIDHasBeenSet = true; m_replicaKmsKeyID = std::move(value); }

    /**
     * The id of the KMS key used to encrypt the replica object.
     */
    inline void SetReplicaKmsKeyID(const char* value) { m_replicaKmsKeyIDHasBeenSet = true; m_replicaKmsKeyID.assign(value); }

    /**
     * The id of the KMS key used to encrypt the replica object.
     */
    inline EncryptionConfiguration& WithReplicaKmsKeyID(const Aws::String& value) { SetReplicaKmsKeyID(value); return *this;}

    /**
     * The id of the KMS key used to encrypt the replica object.
     */
    inline EncryptionConfiguration& WithReplicaKmsKeyID(Aws::String&& value) { SetReplicaKmsKeyID(std::move(value)); return *this;}

    /**
     * The id of the KMS key used to encrypt the replica object.
     */
    inline EncryptionConfiguration& WithReplicaKmsKeyID(const char* value) { SetReplicaKmsKeyID(value); return *this;}

  private:

    Aws::String m_replicaKmsKeyID;
    bool m_replicaKmsKeyIDHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
