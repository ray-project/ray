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
#include <aws/s3/model/NotificationConfigurationFilter.h>
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

  /**
   * Container for specifying an configuration when you want Amazon S3 to publish
   * events to an Amazon Simple Queue Service (Amazon SQS) queue.<p><h3>See
   * Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/QueueConfiguration">AWS
   * API Reference</a></p>
   */
  class AWS_S3_API QueueConfiguration
  {
  public:
    QueueConfiguration();
    QueueConfiguration(const Aws::Utils::Xml::XmlNode& xmlNode);
    QueueConfiguration& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    
    inline const Aws::String& GetId() const{ return m_id; }

    
    inline void SetId(const Aws::String& value) { m_idHasBeenSet = true; m_id = value; }

    
    inline void SetId(Aws::String&& value) { m_idHasBeenSet = true; m_id = std::move(value); }

    
    inline void SetId(const char* value) { m_idHasBeenSet = true; m_id.assign(value); }

    
    inline QueueConfiguration& WithId(const Aws::String& value) { SetId(value); return *this;}

    
    inline QueueConfiguration& WithId(Aws::String&& value) { SetId(std::move(value)); return *this;}

    
    inline QueueConfiguration& WithId(const char* value) { SetId(value); return *this;}


    /**
     * Amazon SQS queue ARN to which Amazon S3 will publish a message when it detects
     * events of specified type.
     */
    inline const Aws::String& GetQueueArn() const{ return m_queueArn; }

    /**
     * Amazon SQS queue ARN to which Amazon S3 will publish a message when it detects
     * events of specified type.
     */
    inline void SetQueueArn(const Aws::String& value) { m_queueArnHasBeenSet = true; m_queueArn = value; }

    /**
     * Amazon SQS queue ARN to which Amazon S3 will publish a message when it detects
     * events of specified type.
     */
    inline void SetQueueArn(Aws::String&& value) { m_queueArnHasBeenSet = true; m_queueArn = std::move(value); }

    /**
     * Amazon SQS queue ARN to which Amazon S3 will publish a message when it detects
     * events of specified type.
     */
    inline void SetQueueArn(const char* value) { m_queueArnHasBeenSet = true; m_queueArn.assign(value); }

    /**
     * Amazon SQS queue ARN to which Amazon S3 will publish a message when it detects
     * events of specified type.
     */
    inline QueueConfiguration& WithQueueArn(const Aws::String& value) { SetQueueArn(value); return *this;}

    /**
     * Amazon SQS queue ARN to which Amazon S3 will publish a message when it detects
     * events of specified type.
     */
    inline QueueConfiguration& WithQueueArn(Aws::String&& value) { SetQueueArn(std::move(value)); return *this;}

    /**
     * Amazon SQS queue ARN to which Amazon S3 will publish a message when it detects
     * events of specified type.
     */
    inline QueueConfiguration& WithQueueArn(const char* value) { SetQueueArn(value); return *this;}


    
    inline const Aws::Vector<Event>& GetEvents() const{ return m_events; }

    
    inline void SetEvents(const Aws::Vector<Event>& value) { m_eventsHasBeenSet = true; m_events = value; }

    
    inline void SetEvents(Aws::Vector<Event>&& value) { m_eventsHasBeenSet = true; m_events = std::move(value); }

    
    inline QueueConfiguration& WithEvents(const Aws::Vector<Event>& value) { SetEvents(value); return *this;}

    
    inline QueueConfiguration& WithEvents(Aws::Vector<Event>&& value) { SetEvents(std::move(value)); return *this;}

    
    inline QueueConfiguration& AddEvents(const Event& value) { m_eventsHasBeenSet = true; m_events.push_back(value); return *this; }

    
    inline QueueConfiguration& AddEvents(Event&& value) { m_eventsHasBeenSet = true; m_events.push_back(std::move(value)); return *this; }


    
    inline const NotificationConfigurationFilter& GetFilter() const{ return m_filter; }

    
    inline void SetFilter(const NotificationConfigurationFilter& value) { m_filterHasBeenSet = true; m_filter = value; }

    
    inline void SetFilter(NotificationConfigurationFilter&& value) { m_filterHasBeenSet = true; m_filter = std::move(value); }

    
    inline QueueConfiguration& WithFilter(const NotificationConfigurationFilter& value) { SetFilter(value); return *this;}

    
    inline QueueConfiguration& WithFilter(NotificationConfigurationFilter&& value) { SetFilter(std::move(value)); return *this;}

  private:

    Aws::String m_id;
    bool m_idHasBeenSet;

    Aws::String m_queueArn;
    bool m_queueArnHasBeenSet;

    Aws::Vector<Event> m_events;
    bool m_eventsHasBeenSet;

    NotificationConfigurationFilter m_filter;
    bool m_filterHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
