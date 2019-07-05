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
#include <aws/s3/model/SseKmsEncryptedObjectsStatus.h>
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
   * Container for filter information of selection of KMS Encrypted S3
   * objects.<p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/SseKmsEncryptedObjects">AWS
   * API Reference</a></p>
   */
  class AWS_S3_API SseKmsEncryptedObjects
  {
  public:
    SseKmsEncryptedObjects();
    SseKmsEncryptedObjects(const Aws::Utils::Xml::XmlNode& xmlNode);
    SseKmsEncryptedObjects& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * The replication for KMS encrypted S3 objects is disabled if status is not
     * Enabled.
     */
    inline const SseKmsEncryptedObjectsStatus& GetStatus() const{ return m_status; }

    /**
     * The replication for KMS encrypted S3 objects is disabled if status is not
     * Enabled.
     */
    inline void SetStatus(const SseKmsEncryptedObjectsStatus& value) { m_statusHasBeenSet = true; m_status = value; }

    /**
     * The replication for KMS encrypted S3 objects is disabled if status is not
     * Enabled.
     */
    inline void SetStatus(SseKmsEncryptedObjectsStatus&& value) { m_statusHasBeenSet = true; m_status = std::move(value); }

    /**
     * The replication for KMS encrypted S3 objects is disabled if status is not
     * Enabled.
     */
    inline SseKmsEncryptedObjects& WithStatus(const SseKmsEncryptedObjectsStatus& value) { SetStatus(value); return *this;}

    /**
     * The replication for KMS encrypted S3 objects is disabled if status is not
     * Enabled.
     */
    inline SseKmsEncryptedObjects& WithStatus(SseKmsEncryptedObjectsStatus&& value) { SetStatus(std::move(value)); return *this;}

  private:

    SseKmsEncryptedObjectsStatus m_status;
    bool m_statusHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
