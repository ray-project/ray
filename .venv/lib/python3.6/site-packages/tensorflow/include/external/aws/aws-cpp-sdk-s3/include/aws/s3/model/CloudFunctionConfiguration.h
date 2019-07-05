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
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/s3/model/Event.h>
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

  class AWS_S3_API CloudFunctionConfiguration
  {
  public:
    CloudFunctionConfiguration();
    CloudFunctionConfiguration(const Aws::Utils::Xml::XmlNode& xmlNode);
    CloudFunctionConfiguration& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    
    inline const Aws::String& GetId() const{ return m_id; }

    
    inline void SetId(const Aws::String& value) { m_idHasBeenSet = true; m_id = value; }

    
    inline void SetId(Aws::String&& value) { m_idHasBeenSet = true; m_id = std::move(value); }

    
    inline void SetId(const char* value) { m_idHasBeenSet = true; m_id.assign(value); }

    
    inline CloudFunctionConfiguration& WithId(const Aws::String& value) { SetId(value); return *this;}

    
    inline CloudFunctionConfiguration& WithId(Aws::String&& value) { SetId(std::move(value)); return *this;}

    
    inline CloudFunctionConfiguration& WithId(const char* value) { SetId(value); return *this;}


    
    inline const Aws::Vector<Event>& GetEvents() const{ return m_events; }

    
    inline void SetEvents(const Aws::Vector<Event>& value) { m_eventsHasBeenSet = true; m_events = value; }

    
    inline void SetEvents(Aws::Vector<Event>&& value) { m_eventsHasBeenSet = true; m_events = std::move(value); }

    
    inline CloudFunctionConfiguration& WithEvents(const Aws::Vector<Event>& value) { SetEvents(value); return *this;}

    
    inline CloudFunctionConfiguration& WithEvents(Aws::Vector<Event>&& value) { SetEvents(std::move(value)); return *this;}

    
    inline CloudFunctionConfiguration& AddEvents(const Event& value) { m_eventsHasBeenSet = true; m_events.push_back(value); return *this; }

    
    inline CloudFunctionConfiguration& AddEvents(Event&& value) { m_eventsHasBeenSet = true; m_events.push_back(std::move(value)); return *this; }


    
    inline const Aws::String& GetCloudFunction() const{ return m_cloudFunction; }

    
    inline void SetCloudFunction(const Aws::String& value) { m_cloudFunctionHasBeenSet = true; m_cloudFunction = value; }

    
    inline void SetCloudFunction(Aws::String&& value) { m_cloudFunctionHasBeenSet = true; m_cloudFunction = std::move(value); }

    
    inline void SetCloudFunction(const char* value) { m_cloudFunctionHasBeenSet = true; m_cloudFunction.assign(value); }

    
    inline CloudFunctionConfiguration& WithCloudFunction(const Aws::String& value) { SetCloudFunction(value); return *this;}

    
    inline CloudFunctionConfiguration& WithCloudFunction(Aws::String&& value) { SetCloudFunction(std::move(value)); return *this;}

    
    inline CloudFunctionConfiguration& WithCloudFunction(const char* value) { SetCloudFunction(value); return *this;}


    
    inline const Aws::String& GetInvocationRole() const{ return m_invocationRole; }

    
    inline void SetInvocationRole(const Aws::String& value) { m_invocationRoleHasBeenSet = true; m_invocationRole = value; }

    
    inline void SetInvocationRole(Aws::String&& value) { m_invocationRoleHasBeenSet = true; m_invocationRole = std::move(value); }

    
    inline void SetInvocationRole(const char* value) { m_invocationRoleHasBeenSet = true; m_invocationRole.assign(value); }

    
    inline CloudFunctionConfiguration& WithInvocationRole(const Aws::String& value) { SetInvocationRole(value); return *this;}

    
    inline CloudFunctionConfiguration& WithInvocationRole(Aws::String&& value) { SetInvocationRole(std::move(value)); return *this;}

    
    inline CloudFunctionConfiguration& WithInvocationRole(const char* value) { SetInvocationRole(value); return *this;}

  private:

    Aws::String m_id;
    bool m_idHasBeenSet;

    Aws::Vector<Event> m_events;
    bool m_eventsHasBeenSet;

    Aws::String m_cloudFunction;
    bool m_cloudFunctionHasBeenSet;

    Aws::String m_invocationRole;
    bool m_invocationRoleHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
