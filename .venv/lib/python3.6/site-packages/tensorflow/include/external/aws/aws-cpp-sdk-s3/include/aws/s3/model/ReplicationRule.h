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
#include <aws/s3/model/ReplicationRuleStatus.h>
#include <aws/s3/model/SourceSelectionCriteria.h>
#include <aws/s3/model/Destination.h>
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
   * Container for information about a particular replication rule.<p><h3>See
   * Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/ReplicationRule">AWS
   * API Reference</a></p>
   */
  class AWS_S3_API ReplicationRule
  {
  public:
    ReplicationRule();
    ReplicationRule(const Aws::Utils::Xml::XmlNode& xmlNode);
    ReplicationRule& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * Unique identifier for the rule. The value cannot be longer than 255 characters.
     */
    inline const Aws::String& GetID() const{ return m_iD; }

    /**
     * Unique identifier for the rule. The value cannot be longer than 255 characters.
     */
    inline void SetID(const Aws::String& value) { m_iDHasBeenSet = true; m_iD = value; }

    /**
     * Unique identifier for the rule. The value cannot be longer than 255 characters.
     */
    inline void SetID(Aws::String&& value) { m_iDHasBeenSet = true; m_iD = std::move(value); }

    /**
     * Unique identifier for the rule. The value cannot be longer than 255 characters.
     */
    inline void SetID(const char* value) { m_iDHasBeenSet = true; m_iD.assign(value); }

    /**
     * Unique identifier for the rule. The value cannot be longer than 255 characters.
     */
    inline ReplicationRule& WithID(const Aws::String& value) { SetID(value); return *this;}

    /**
     * Unique identifier for the rule. The value cannot be longer than 255 characters.
     */
    inline ReplicationRule& WithID(Aws::String&& value) { SetID(std::move(value)); return *this;}

    /**
     * Unique identifier for the rule. The value cannot be longer than 255 characters.
     */
    inline ReplicationRule& WithID(const char* value) { SetID(value); return *this;}


    /**
     * Object keyname prefix identifying one or more objects to which the rule applies.
     * Maximum prefix length can be up to 1,024 characters. Overlapping prefixes are
     * not supported.
     */
    inline const Aws::String& GetPrefix() const{ return m_prefix; }

    /**
     * Object keyname prefix identifying one or more objects to which the rule applies.
     * Maximum prefix length can be up to 1,024 characters. Overlapping prefixes are
     * not supported.
     */
    inline void SetPrefix(const Aws::String& value) { m_prefixHasBeenSet = true; m_prefix = value; }

    /**
     * Object keyname prefix identifying one or more objects to which the rule applies.
     * Maximum prefix length can be up to 1,024 characters. Overlapping prefixes are
     * not supported.
     */
    inline void SetPrefix(Aws::String&& value) { m_prefixHasBeenSet = true; m_prefix = std::move(value); }

    /**
     * Object keyname prefix identifying one or more objects to which the rule applies.
     * Maximum prefix length can be up to 1,024 characters. Overlapping prefixes are
     * not supported.
     */
    inline void SetPrefix(const char* value) { m_prefixHasBeenSet = true; m_prefix.assign(value); }

    /**
     * Object keyname prefix identifying one or more objects to which the rule applies.
     * Maximum prefix length can be up to 1,024 characters. Overlapping prefixes are
     * not supported.
     */
    inline ReplicationRule& WithPrefix(const Aws::String& value) { SetPrefix(value); return *this;}

    /**
     * Object keyname prefix identifying one or more objects to which the rule applies.
     * Maximum prefix length can be up to 1,024 characters. Overlapping prefixes are
     * not supported.
     */
    inline ReplicationRule& WithPrefix(Aws::String&& value) { SetPrefix(std::move(value)); return *this;}

    /**
     * Object keyname prefix identifying one or more objects to which the rule applies.
     * Maximum prefix length can be up to 1,024 characters. Overlapping prefixes are
     * not supported.
     */
    inline ReplicationRule& WithPrefix(const char* value) { SetPrefix(value); return *this;}


    /**
     * The rule is ignored if status is not Enabled.
     */
    inline const ReplicationRuleStatus& GetStatus() const{ return m_status; }

    /**
     * The rule is ignored if status is not Enabled.
     */
    inline void SetStatus(const ReplicationRuleStatus& value) { m_statusHasBeenSet = true; m_status = value; }

    /**
     * The rule is ignored if status is not Enabled.
     */
    inline void SetStatus(ReplicationRuleStatus&& value) { m_statusHasBeenSet = true; m_status = std::move(value); }

    /**
     * The rule is ignored if status is not Enabled.
     */
    inline ReplicationRule& WithStatus(const ReplicationRuleStatus& value) { SetStatus(value); return *this;}

    /**
     * The rule is ignored if status is not Enabled.
     */
    inline ReplicationRule& WithStatus(ReplicationRuleStatus&& value) { SetStatus(std::move(value)); return *this;}


    /**
     * Container for filters that define which source objects should be replicated.
     */
    inline const SourceSelectionCriteria& GetSourceSelectionCriteria() const{ return m_sourceSelectionCriteria; }

    /**
     * Container for filters that define which source objects should be replicated.
     */
    inline void SetSourceSelectionCriteria(const SourceSelectionCriteria& value) { m_sourceSelectionCriteriaHasBeenSet = true; m_sourceSelectionCriteria = value; }

    /**
     * Container for filters that define which source objects should be replicated.
     */
    inline void SetSourceSelectionCriteria(SourceSelectionCriteria&& value) { m_sourceSelectionCriteriaHasBeenSet = true; m_sourceSelectionCriteria = std::move(value); }

    /**
     * Container for filters that define which source objects should be replicated.
     */
    inline ReplicationRule& WithSourceSelectionCriteria(const SourceSelectionCriteria& value) { SetSourceSelectionCriteria(value); return *this;}

    /**
     * Container for filters that define which source objects should be replicated.
     */
    inline ReplicationRule& WithSourceSelectionCriteria(SourceSelectionCriteria&& value) { SetSourceSelectionCriteria(std::move(value)); return *this;}


    /**
     * Container for replication destination information.
     */
    inline const Destination& GetDestination() const{ return m_destination; }

    /**
     * Container for replication destination information.
     */
    inline void SetDestination(const Destination& value) { m_destinationHasBeenSet = true; m_destination = value; }

    /**
     * Container for replication destination information.
     */
    inline void SetDestination(Destination&& value) { m_destinationHasBeenSet = true; m_destination = std::move(value); }

    /**
     * Container for replication destination information.
     */
    inline ReplicationRule& WithDestination(const Destination& value) { SetDestination(value); return *this;}

    /**
     * Container for replication destination information.
     */
    inline ReplicationRule& WithDestination(Destination&& value) { SetDestination(std::move(value)); return *this;}

  private:

    Aws::String m_iD;
    bool m_iDHasBeenSet;

    Aws::String m_prefix;
    bool m_prefixHasBeenSet;

    ReplicationRuleStatus m_status;
    bool m_statusHasBeenSet;

    SourceSelectionCriteria m_sourceSelectionCriteria;
    bool m_sourceSelectionCriteriaHasBeenSet;

    Destination m_destination;
    bool m_destinationHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
