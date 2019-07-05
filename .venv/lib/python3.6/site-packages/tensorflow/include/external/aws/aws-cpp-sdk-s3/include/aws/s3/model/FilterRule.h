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
#include <aws/s3/model/FilterRuleName.h>
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
   * Container for key value pair that defines the criteria for the filter
   * rule.<p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/FilterRule">AWS API
   * Reference</a></p>
   */
  class AWS_S3_API FilterRule
  {
  public:
    FilterRule();
    FilterRule(const Aws::Utils::Xml::XmlNode& xmlNode);
    FilterRule& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * Object key name prefix or suffix identifying one or more objects to which the
     * filtering rule applies. Maximum prefix length can be up to 1,024 characters.
     * Overlapping prefixes and suffixes are not supported. For more information, go to
     * <a
     * href="http://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html">Configuring
     * Event Notifications</a> in the Amazon Simple Storage Service Developer Guide.
     */
    inline const FilterRuleName& GetName() const{ return m_name; }

    /**
     * Object key name prefix or suffix identifying one or more objects to which the
     * filtering rule applies. Maximum prefix length can be up to 1,024 characters.
     * Overlapping prefixes and suffixes are not supported. For more information, go to
     * <a
     * href="http://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html">Configuring
     * Event Notifications</a> in the Amazon Simple Storage Service Developer Guide.
     */
    inline void SetName(const FilterRuleName& value) { m_nameHasBeenSet = true; m_name = value; }

    /**
     * Object key name prefix or suffix identifying one or more objects to which the
     * filtering rule applies. Maximum prefix length can be up to 1,024 characters.
     * Overlapping prefixes and suffixes are not supported. For more information, go to
     * <a
     * href="http://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html">Configuring
     * Event Notifications</a> in the Amazon Simple Storage Service Developer Guide.
     */
    inline void SetName(FilterRuleName&& value) { m_nameHasBeenSet = true; m_name = std::move(value); }

    /**
     * Object key name prefix or suffix identifying one or more objects to which the
     * filtering rule applies. Maximum prefix length can be up to 1,024 characters.
     * Overlapping prefixes and suffixes are not supported. For more information, go to
     * <a
     * href="http://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html">Configuring
     * Event Notifications</a> in the Amazon Simple Storage Service Developer Guide.
     */
    inline FilterRule& WithName(const FilterRuleName& value) { SetName(value); return *this;}

    /**
     * Object key name prefix or suffix identifying one or more objects to which the
     * filtering rule applies. Maximum prefix length can be up to 1,024 characters.
     * Overlapping prefixes and suffixes are not supported. For more information, go to
     * <a
     * href="http://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html">Configuring
     * Event Notifications</a> in the Amazon Simple Storage Service Developer Guide.
     */
    inline FilterRule& WithName(FilterRuleName&& value) { SetName(std::move(value)); return *this;}


    
    inline const Aws::String& GetValue() const{ return m_value; }

    
    inline void SetValue(const Aws::String& value) { m_valueHasBeenSet = true; m_value = value; }

    
    inline void SetValue(Aws::String&& value) { m_valueHasBeenSet = true; m_value = std::move(value); }

    
    inline void SetValue(const char* value) { m_valueHasBeenSet = true; m_value.assign(value); }

    
    inline FilterRule& WithValue(const Aws::String& value) { SetValue(value); return *this;}

    
    inline FilterRule& WithValue(Aws::String&& value) { SetValue(std::move(value)); return *this;}

    
    inline FilterRule& WithValue(const char* value) { SetValue(value); return *this;}

  private:

    FilterRuleName m_name;
    bool m_nameHasBeenSet;

    Aws::String m_value;
    bool m_valueHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
