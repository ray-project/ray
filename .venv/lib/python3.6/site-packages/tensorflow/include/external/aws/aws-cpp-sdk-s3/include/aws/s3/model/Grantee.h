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
#include <aws/s3/model/Type.h>
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

  class AWS_S3_API Grantee
  {
  public:
    Grantee();
    Grantee(const Aws::Utils::Xml::XmlNode& xmlNode);
    Grantee& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * Screen name of the grantee.
     */
    inline const Aws::String& GetDisplayName() const{ return m_displayName; }

    /**
     * Screen name of the grantee.
     */
    inline void SetDisplayName(const Aws::String& value) { m_displayNameHasBeenSet = true; m_displayName = value; }

    /**
     * Screen name of the grantee.
     */
    inline void SetDisplayName(Aws::String&& value) { m_displayNameHasBeenSet = true; m_displayName = std::move(value); }

    /**
     * Screen name of the grantee.
     */
    inline void SetDisplayName(const char* value) { m_displayNameHasBeenSet = true; m_displayName.assign(value); }

    /**
     * Screen name of the grantee.
     */
    inline Grantee& WithDisplayName(const Aws::String& value) { SetDisplayName(value); return *this;}

    /**
     * Screen name of the grantee.
     */
    inline Grantee& WithDisplayName(Aws::String&& value) { SetDisplayName(std::move(value)); return *this;}

    /**
     * Screen name of the grantee.
     */
    inline Grantee& WithDisplayName(const char* value) { SetDisplayName(value); return *this;}


    /**
     * Email address of the grantee.
     */
    inline const Aws::String& GetEmailAddress() const{ return m_emailAddress; }

    /**
     * Email address of the grantee.
     */
    inline void SetEmailAddress(const Aws::String& value) { m_emailAddressHasBeenSet = true; m_emailAddress = value; }

    /**
     * Email address of the grantee.
     */
    inline void SetEmailAddress(Aws::String&& value) { m_emailAddressHasBeenSet = true; m_emailAddress = std::move(value); }

    /**
     * Email address of the grantee.
     */
    inline void SetEmailAddress(const char* value) { m_emailAddressHasBeenSet = true; m_emailAddress.assign(value); }

    /**
     * Email address of the grantee.
     */
    inline Grantee& WithEmailAddress(const Aws::String& value) { SetEmailAddress(value); return *this;}

    /**
     * Email address of the grantee.
     */
    inline Grantee& WithEmailAddress(Aws::String&& value) { SetEmailAddress(std::move(value)); return *this;}

    /**
     * Email address of the grantee.
     */
    inline Grantee& WithEmailAddress(const char* value) { SetEmailAddress(value); return *this;}


    /**
     * The canonical user ID of the grantee.
     */
    inline const Aws::String& GetID() const{ return m_iD; }

    /**
     * The canonical user ID of the grantee.
     */
    inline void SetID(const Aws::String& value) { m_iDHasBeenSet = true; m_iD = value; }

    /**
     * The canonical user ID of the grantee.
     */
    inline void SetID(Aws::String&& value) { m_iDHasBeenSet = true; m_iD = std::move(value); }

    /**
     * The canonical user ID of the grantee.
     */
    inline void SetID(const char* value) { m_iDHasBeenSet = true; m_iD.assign(value); }

    /**
     * The canonical user ID of the grantee.
     */
    inline Grantee& WithID(const Aws::String& value) { SetID(value); return *this;}

    /**
     * The canonical user ID of the grantee.
     */
    inline Grantee& WithID(Aws::String&& value) { SetID(std::move(value)); return *this;}

    /**
     * The canonical user ID of the grantee.
     */
    inline Grantee& WithID(const char* value) { SetID(value); return *this;}


    /**
     * Type of grantee
     */
    inline const Type& GetType() const{ return m_type; }

    /**
     * Type of grantee
     */
    inline void SetType(const Type& value) { m_typeHasBeenSet = true; m_type = value; }

    /**
     * Type of grantee
     */
    inline void SetType(Type&& value) { m_typeHasBeenSet = true; m_type = std::move(value); }

    /**
     * Type of grantee
     */
    inline Grantee& WithType(const Type& value) { SetType(value); return *this;}

    /**
     * Type of grantee
     */
    inline Grantee& WithType(Type&& value) { SetType(std::move(value)); return *this;}


    /**
     * URI of the grantee group.
     */
    inline const Aws::String& GetURI() const{ return m_uRI; }

    /**
     * URI of the grantee group.
     */
    inline void SetURI(const Aws::String& value) { m_uRIHasBeenSet = true; m_uRI = value; }

    /**
     * URI of the grantee group.
     */
    inline void SetURI(Aws::String&& value) { m_uRIHasBeenSet = true; m_uRI = std::move(value); }

    /**
     * URI of the grantee group.
     */
    inline void SetURI(const char* value) { m_uRIHasBeenSet = true; m_uRI.assign(value); }

    /**
     * URI of the grantee group.
     */
    inline Grantee& WithURI(const Aws::String& value) { SetURI(value); return *this;}

    /**
     * URI of the grantee group.
     */
    inline Grantee& WithURI(Aws::String&& value) { SetURI(std::move(value)); return *this;}

    /**
     * URI of the grantee group.
     */
    inline Grantee& WithURI(const char* value) { SetURI(value); return *this;}

  private:

    Aws::String m_displayName;
    bool m_displayNameHasBeenSet;

    Aws::String m_emailAddress;
    bool m_emailAddressHasBeenSet;

    Aws::String m_iD;
    bool m_iDHasBeenSet;

    Type m_type;
    bool m_typeHasBeenSet;

    Aws::String m_uRI;
    bool m_uRIHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
