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

  class AWS_S3_API QueueConfigurationDeprecated
  {
  public:
    QueueConfigurationDeprecated();
    QueueConfigurationDeprecated(const Aws::Utils::Xml::XmlNode& xmlNode);
    QueueConfigurationDeprecated& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    
    inline const Aws::String& GetId() const{ return m_id; }

    
    inline void SetId(const Aws::String& value) { m_idHasBeenSet = true; m_id = value; }

    
    inline void SetId(Aws::String&& value) { m_idHasBeenSet = true; m_id = std::move(value); }

    
    inline void SetId(const char* value) { m_idHasBeenSet = true; m_id.assign(value); }

    
    inline QueueConfigurationDeprecated& WithId(const Aws::String& value) { SetId(value); return *this;}

    
    inline QueueConfigurationDeprecated& WithId(Aws::String&& value) { SetId(std::move(value)); return *this;}

    
    inline QueueConfigurationDeprecated& WithId(const char* value) { SetId(value); return *this;}


    
    inline const Aws::Vector<Event>& GetEvents() const{ return m_events; }

    
    inline void SetEvents(const Aws::Vector<Event>& value) { m_eventsHasBeenSet = true; m_events = value; }

    
    inline void SetEvents(Aws::Vector<Event>&& value) { m_eventsHasBeenSet = true; m_events = std::move(value); }

    
    inline QueueConfigurationDeprecated& WithEvents(const Aws::Vector<Event>& value) { SetEvents(value); return *this;}

    
    inline QueueConfigurationDeprecated& WithEvents(Aws::Vector<Event>&& value) { SetEvents(std::move(value)); return *this;}

    
    inline QueueConfigurationDeprecated& AddEvents(const Event& value) { m_eventsHasBeenSet = true; m_events.push_back(value); return *this; }

    
    inline QueueConfigurationDeprecated& AddEvents(Event&& value) { m_eventsHasBeenSet = true; m_events.push_back(std::move(value)); return *this; }


    
    inline const Aws::String& GetQueue() const{ return m_queue; }

    
    inline void SetQueue(const Aws::String& value) { m_queueHasBeenSet = true; m_queue = value; }

    
    inline void SetQueue(Aws::String&& value) { m_queueHasBeenSet = true; m_queue = std::move(value); }

    
    inline void SetQueue(const char* value) { m_queueHasBeenSet = true; m_queue.assign(value); }

    
    inline QueueConfigurationDeprecated& WithQueue(const Aws::String& value) { SetQueue(value); return *this;}

    
    inline QueueConfigurationDeprecated& WithQueue(Aws::String&& value) { SetQueue(std::move(value)); return *this;}

    
    inline QueueConfigurationDeprecated& WithQueue(const char* value) { SetQueue(value); return *this;}

  private:

    Aws::String m_id;
    bool m_idHasBeenSet;

    Aws::Vector<Event> m_events;
    bool m_eventsHasBeenSet;

    Aws::String m_queue;
    bool m_queueHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
