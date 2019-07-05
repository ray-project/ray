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
#include <aws/s3/model/SseKmsEncryptedObjects.h>
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
   * Container for filters that define which source objects should be
   * replicated.<p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/SourceSelectionCriteria">AWS
   * API Reference</a></p>
   */
  class AWS_S3_API SourceSelectionCriteria
  {
  public:
    SourceSelectionCriteria();
    SourceSelectionCriteria(const Aws::Utils::Xml::XmlNode& xmlNode);
    SourceSelectionCriteria& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * Container for filter information of selection of KMS Encrypted S3 objects.
     */
    inline const SseKmsEncryptedObjects& GetSseKmsEncryptedObjects() const{ return m_sseKmsEncryptedObjects; }

    /**
     * Container for filter information of selection of KMS Encrypted S3 objects.
     */
    inline void SetSseKmsEncryptedObjects(const SseKmsEncryptedObjects& value) { m_sseKmsEncryptedObjectsHasBeenSet = true; m_sseKmsEncryptedObjects = value; }

    /**
     * Container for filter information of selection of KMS Encrypted S3 objects.
     */
    inline void SetSseKmsEncryptedObjects(SseKmsEncryptedObjects&& value) { m_sseKmsEncryptedObjectsHasBeenSet = true; m_sseKmsEncryptedObjects = std::move(value); }

    /**
     * Container for filter information of selection of KMS Encrypted S3 objects.
     */
    inline SourceSelectionCriteria& WithSseKmsEncryptedObjects(const SseKmsEncryptedObjects& value) { SetSseKmsEncryptedObjects(value); return *this;}

    /**
     * Container for filter information of selection of KMS Encrypted S3 objects.
     */
    inline SourceSelectionCriteria& WithSseKmsEncryptedObjects(SseKmsEncryptedObjects&& value) { SetSseKmsEncryptedObjects(std::move(value)); return *this;}

  private:

    SseKmsEncryptedObjects m_sseKmsEncryptedObjects;
    bool m_sseKmsEncryptedObjectsHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
