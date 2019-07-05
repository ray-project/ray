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
#include <aws/s3/model/ServerSideEncryptionRule.h>
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
   * Container for server-side encryption configuration rules. Currently S3 supports
   * one rule only.<p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/ServerSideEncryptionConfiguration">AWS
   * API Reference</a></p>
   */
  class AWS_S3_API ServerSideEncryptionConfiguration
  {
  public:
    ServerSideEncryptionConfiguration();
    ServerSideEncryptionConfiguration(const Aws::Utils::Xml::XmlNode& xmlNode);
    ServerSideEncryptionConfiguration& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * Container for information about a particular server-side encryption
     * configuration rule.
     */
    inline const Aws::Vector<ServerSideEncryptionRule>& GetRules() const{ return m_rules; }

    /**
     * Container for information about a particular server-side encryption
     * configuration rule.
     */
    inline void SetRules(const Aws::Vector<ServerSideEncryptionRule>& value) { m_rulesHasBeenSet = true; m_rules = value; }

    /**
     * Container for information about a particular server-side encryption
     * configuration rule.
     */
    inline void SetRules(Aws::Vector<ServerSideEncryptionRule>&& value) { m_rulesHasBeenSet = true; m_rules = std::move(value); }

    /**
     * Container for information about a particular server-side encryption
     * configuration rule.
     */
    inline ServerSideEncryptionConfiguration& WithRules(const Aws::Vector<ServerSideEncryptionRule>& value) { SetRules(value); return *this;}

    /**
     * Container for information about a particular server-side encryption
     * configuration rule.
     */
    inline ServerSideEncryptionConfiguration& WithRules(Aws::Vector<ServerSideEncryptionRule>&& value) { SetRules(std::move(value)); return *this;}

    /**
     * Container for information about a particular server-side encryption
     * configuration rule.
     */
    inline ServerSideEncryptionConfiguration& AddRules(const ServerSideEncryptionRule& value) { m_rulesHasBeenSet = true; m_rules.push_back(value); return *this; }

    /**
     * Container for information about a particular server-side encryption
     * configuration rule.
     */
    inline ServerSideEncryptionConfiguration& AddRules(ServerSideEncryptionRule&& value) { m_rulesHasBeenSet = true; m_rules.push_back(std::move(value)); return *this; }

  private:

    Aws::Vector<ServerSideEncryptionRule> m_rules;
    bool m_rulesHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
