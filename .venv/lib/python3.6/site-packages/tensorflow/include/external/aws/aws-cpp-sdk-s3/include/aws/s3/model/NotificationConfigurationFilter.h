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
#include <aws/s3/model/S3KeyFilter.h>
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
   * Container for object key name filtering rules. For information about key name
   * filtering, go to <a
   * href="http://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html">Configuring
   * Event Notifications</a> in the Amazon Simple Storage Service Developer
   * Guide.<p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/NotificationConfigurationFilter">AWS
   * API Reference</a></p>
   */
  class AWS_S3_API NotificationConfigurationFilter
  {
  public:
    NotificationConfigurationFilter();
    NotificationConfigurationFilter(const Aws::Utils::Xml::XmlNode& xmlNode);
    NotificationConfigurationFilter& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    
    inline const S3KeyFilter& GetKey() const{ return m_key; }

    
    inline void SetKey(const S3KeyFilter& value) { m_keyHasBeenSet = true; m_key = value; }

    
    inline void SetKey(S3KeyFilter&& value) { m_keyHasBeenSet = true; m_key = std::move(value); }

    
    inline NotificationConfigurationFilter& WithKey(const S3KeyFilter& value) { SetKey(value); return *this;}

    
    inline NotificationConfigurationFilter& WithKey(S3KeyFilter&& value) { SetKey(std::move(value)); return *this;}

  private:

    S3KeyFilter m_key;
    bool m_keyHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
