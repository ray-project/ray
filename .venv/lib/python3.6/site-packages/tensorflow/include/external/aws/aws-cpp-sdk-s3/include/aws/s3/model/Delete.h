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
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/s3/model/ObjectIdentifier.h>
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

  class AWS_S3_API Delete
  {
  public:
    Delete();
    Delete(const Aws::Utils::Xml::XmlNode& xmlNode);
    Delete& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    
    inline const Aws::Vector<ObjectIdentifier>& GetObjects() const{ return m_objects; }

    
    inline void SetObjects(const Aws::Vector<ObjectIdentifier>& value) { m_objectsHasBeenSet = true; m_objects = value; }

    
    inline void SetObjects(Aws::Vector<ObjectIdentifier>&& value) { m_objectsHasBeenSet = true; m_objects = std::move(value); }

    
    inline Delete& WithObjects(const Aws::Vector<ObjectIdentifier>& value) { SetObjects(value); return *this;}

    
    inline Delete& WithObjects(Aws::Vector<ObjectIdentifier>&& value) { SetObjects(std::move(value)); return *this;}

    
    inline Delete& AddObjects(const ObjectIdentifier& value) { m_objectsHasBeenSet = true; m_objects.push_back(value); return *this; }

    
    inline Delete& AddObjects(ObjectIdentifier&& value) { m_objectsHasBeenSet = true; m_objects.push_back(std::move(value)); return *this; }


    /**
     * Element to enable quiet mode for the request. When you add this element, you
     * must set its value to true.
     */
    inline bool GetQuiet() const{ return m_quiet; }

    /**
     * Element to enable quiet mode for the request. When you add this element, you
     * must set its value to true.
     */
    inline void SetQuiet(bool value) { m_quietHasBeenSet = true; m_quiet = value; }

    /**
     * Element to enable quiet mode for the request. When you add this element, you
     * must set its value to true.
     */
    inline Delete& WithQuiet(bool value) { SetQuiet(value); return *this;}

  private:

    Aws::Vector<ObjectIdentifier> m_objects;
    bool m_objectsHasBeenSet;

    bool m_quiet;
    bool m_quietHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
