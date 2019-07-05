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
#include <aws/s3/model/InventoryDestination.h>
#include <aws/s3/model/InventoryFilter.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/s3/model/InventoryIncludedObjectVersions.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/s3/model/InventorySchedule.h>
#include <aws/s3/model/InventoryOptionalField.h>
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

  class AWS_S3_API InventoryConfiguration
  {
  public:
    InventoryConfiguration();
    InventoryConfiguration(const Aws::Utils::Xml::XmlNode& xmlNode);
    InventoryConfiguration& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * Contains information about where to publish the inventory results.
     */
    inline const InventoryDestination& GetDestination() const{ return m_destination; }

    /**
     * Contains information about where to publish the inventory results.
     */
    inline void SetDestination(const InventoryDestination& value) { m_destinationHasBeenSet = true; m_destination = value; }

    /**
     * Contains information about where to publish the inventory results.
     */
    inline void SetDestination(InventoryDestination&& value) { m_destinationHasBeenSet = true; m_destination = std::move(value); }

    /**
     * Contains information about where to publish the inventory results.
     */
    inline InventoryConfiguration& WithDestination(const InventoryDestination& value) { SetDestination(value); return *this;}

    /**
     * Contains information about where to publish the inventory results.
     */
    inline InventoryConfiguration& WithDestination(InventoryDestination&& value) { SetDestination(std::move(value)); return *this;}


    /**
     * Specifies whether the inventory is enabled or disabled.
     */
    inline bool GetIsEnabled() const{ return m_isEnabled; }

    /**
     * Specifies whether the inventory is enabled or disabled.
     */
    inline void SetIsEnabled(bool value) { m_isEnabledHasBeenSet = true; m_isEnabled = value; }

    /**
     * Specifies whether the inventory is enabled or disabled.
     */
    inline InventoryConfiguration& WithIsEnabled(bool value) { SetIsEnabled(value); return *this;}


    /**
     * Specifies an inventory filter. The inventory only includes objects that meet the
     * filter's criteria.
     */
    inline const InventoryFilter& GetFilter() const{ return m_filter; }

    /**
     * Specifies an inventory filter. The inventory only includes objects that meet the
     * filter's criteria.
     */
    inline void SetFilter(const InventoryFilter& value) { m_filterHasBeenSet = true; m_filter = value; }

    /**
     * Specifies an inventory filter. The inventory only includes objects that meet the
     * filter's criteria.
     */
    inline void SetFilter(InventoryFilter&& value) { m_filterHasBeenSet = true; m_filter = std::move(value); }

    /**
     * Specifies an inventory filter. The inventory only includes objects that meet the
     * filter's criteria.
     */
    inline InventoryConfiguration& WithFilter(const InventoryFilter& value) { SetFilter(value); return *this;}

    /**
     * Specifies an inventory filter. The inventory only includes objects that meet the
     * filter's criteria.
     */
    inline InventoryConfiguration& WithFilter(InventoryFilter&& value) { SetFilter(std::move(value)); return *this;}


    /**
     * The ID used to identify the inventory configuration.
     */
    inline const Aws::String& GetId() const{ return m_id; }

    /**
     * The ID used to identify the inventory configuration.
     */
    inline void SetId(const Aws::String& value) { m_idHasBeenSet = true; m_id = value; }

    /**
     * The ID used to identify the inventory configuration.
     */
    inline void SetId(Aws::String&& value) { m_idHasBeenSet = true; m_id = std::move(value); }

    /**
     * The ID used to identify the inventory configuration.
     */
    inline void SetId(const char* value) { m_idHasBeenSet = true; m_id.assign(value); }

    /**
     * The ID used to identify the inventory configuration.
     */
    inline InventoryConfiguration& WithId(const Aws::String& value) { SetId(value); return *this;}

    /**
     * The ID used to identify the inventory configuration.
     */
    inline InventoryConfiguration& WithId(Aws::String&& value) { SetId(std::move(value)); return *this;}

    /**
     * The ID used to identify the inventory configuration.
     */
    inline InventoryConfiguration& WithId(const char* value) { SetId(value); return *this;}


    /**
     * Specifies which object version(s) to included in the inventory results.
     */
    inline const InventoryIncludedObjectVersions& GetIncludedObjectVersions() const{ return m_includedObjectVersions; }

    /**
     * Specifies which object version(s) to included in the inventory results.
     */
    inline void SetIncludedObjectVersions(const InventoryIncludedObjectVersions& value) { m_includedObjectVersionsHasBeenSet = true; m_includedObjectVersions = value; }

    /**
     * Specifies which object version(s) to included in the inventory results.
     */
    inline void SetIncludedObjectVersions(InventoryIncludedObjectVersions&& value) { m_includedObjectVersionsHasBeenSet = true; m_includedObjectVersions = std::move(value); }

    /**
     * Specifies which object version(s) to included in the inventory results.
     */
    inline InventoryConfiguration& WithIncludedObjectVersions(const InventoryIncludedObjectVersions& value) { SetIncludedObjectVersions(value); return *this;}

    /**
     * Specifies which object version(s) to included in the inventory results.
     */
    inline InventoryConfiguration& WithIncludedObjectVersions(InventoryIncludedObjectVersions&& value) { SetIncludedObjectVersions(std::move(value)); return *this;}


    /**
     * Contains the optional fields that are included in the inventory results.
     */
    inline const Aws::Vector<InventoryOptionalField>& GetOptionalFields() const{ return m_optionalFields; }

    /**
     * Contains the optional fields that are included in the inventory results.
     */
    inline void SetOptionalFields(const Aws::Vector<InventoryOptionalField>& value) { m_optionalFieldsHasBeenSet = true; m_optionalFields = value; }

    /**
     * Contains the optional fields that are included in the inventory results.
     */
    inline void SetOptionalFields(Aws::Vector<InventoryOptionalField>&& value) { m_optionalFieldsHasBeenSet = true; m_optionalFields = std::move(value); }

    /**
     * Contains the optional fields that are included in the inventory results.
     */
    inline InventoryConfiguration& WithOptionalFields(const Aws::Vector<InventoryOptionalField>& value) { SetOptionalFields(value); return *this;}

    /**
     * Contains the optional fields that are included in the inventory results.
     */
    inline InventoryConfiguration& WithOptionalFields(Aws::Vector<InventoryOptionalField>&& value) { SetOptionalFields(std::move(value)); return *this;}

    /**
     * Contains the optional fields that are included in the inventory results.
     */
    inline InventoryConfiguration& AddOptionalFields(const InventoryOptionalField& value) { m_optionalFieldsHasBeenSet = true; m_optionalFields.push_back(value); return *this; }

    /**
     * Contains the optional fields that are included in the inventory results.
     */
    inline InventoryConfiguration& AddOptionalFields(InventoryOptionalField&& value) { m_optionalFieldsHasBeenSet = true; m_optionalFields.push_back(std::move(value)); return *this; }


    /**
     * Specifies the schedule for generating inventory results.
     */
    inline const InventorySchedule& GetSchedule() const{ return m_schedule; }

    /**
     * Specifies the schedule for generating inventory results.
     */
    inline void SetSchedule(const InventorySchedule& value) { m_scheduleHasBeenSet = true; m_schedule = value; }

    /**
     * Specifies the schedule for generating inventory results.
     */
    inline void SetSchedule(InventorySchedule&& value) { m_scheduleHasBeenSet = true; m_schedule = std::move(value); }

    /**
     * Specifies the schedule for generating inventory results.
     */
    inline InventoryConfiguration& WithSchedule(const InventorySchedule& value) { SetSchedule(value); return *this;}

    /**
     * Specifies the schedule for generating inventory results.
     */
    inline InventoryConfiguration& WithSchedule(InventorySchedule&& value) { SetSchedule(std::move(value)); return *this;}

  private:

    InventoryDestination m_destination;
    bool m_destinationHasBeenSet;

    bool m_isEnabled;
    bool m_isEnabledHasBeenSet;

    InventoryFilter m_filter;
    bool m_filterHasBeenSet;

    Aws::String m_id;
    bool m_idHasBeenSet;

    InventoryIncludedObjectVersions m_includedObjectVersions;
    bool m_includedObjectVersionsHasBeenSet;

    Aws::Vector<InventoryOptionalField> m_optionalFields;
    bool m_optionalFieldsHasBeenSet;

    InventorySchedule m_schedule;
    bool m_scheduleHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
