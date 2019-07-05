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

  class AWS_S3_API ErrorDocument
  {
  public:
    ErrorDocument();
    ErrorDocument(const Aws::Utils::Xml::XmlNode& xmlNode);
    ErrorDocument& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * The object key name to use when a 4XX class error occurs.
     */
    inline const Aws::String& GetKey() const{ return m_key; }

    /**
     * The object key name to use when a 4XX class error occurs.
     */
    inline void SetKey(const Aws::String& value) { m_keyHasBeenSet = true; m_key = value; }

    /**
     * The object key name to use when a 4XX class error occurs.
     */
    inline void SetKey(Aws::String&& value) { m_keyHasBeenSet = true; m_key = std::move(value); }

    /**
     * The object key name to use when a 4XX class error occurs.
     */
    inline void SetKey(const char* value) { m_keyHasBeenSet = true; m_key.assign(value); }

    /**
     * The object key name to use when a 4XX class error occurs.
     */
    inline ErrorDocument& WithKey(const Aws::String& value) { SetKey(value); return *this;}

    /**
     * The object key name to use when a 4XX class error occurs.
     */
    inline ErrorDocument& WithKey(Aws::String&& value) { SetKey(std::move(value)); return *this;}

    /**
     * The object key name to use when a 4XX class error occurs.
     */
    inline ErrorDocument& WithKey(const char* value) { SetKey(value); return *this;}

  private:

    Aws::String m_key;
    bool m_keyHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
