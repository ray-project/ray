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
#include <aws/s3/model/StorageClassAnalysisSchemaVersion.h>
#include <aws/s3/model/AnalyticsExportDestination.h>
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

  class AWS_S3_API StorageClassAnalysisDataExport
  {
  public:
    StorageClassAnalysisDataExport();
    StorageClassAnalysisDataExport(const Aws::Utils::Xml::XmlNode& xmlNode);
    StorageClassAnalysisDataExport& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * The version of the output schema to use when exporting data. Must be V_1.
     */
    inline const StorageClassAnalysisSchemaVersion& GetOutputSchemaVersion() const{ return m_outputSchemaVersion; }

    /**
     * The version of the output schema to use when exporting data. Must be V_1.
     */
    inline void SetOutputSchemaVersion(const StorageClassAnalysisSchemaVersion& value) { m_outputSchemaVersionHasBeenSet = true; m_outputSchemaVersion = value; }

    /**
     * The version of the output schema to use when exporting data. Must be V_1.
     */
    inline void SetOutputSchemaVersion(StorageClassAnalysisSchemaVersion&& value) { m_outputSchemaVersionHasBeenSet = true; m_outputSchemaVersion = std::move(value); }

    /**
     * The version of the output schema to use when exporting data. Must be V_1.
     */
    inline StorageClassAnalysisDataExport& WithOutputSchemaVersion(const StorageClassAnalysisSchemaVersion& value) { SetOutputSchemaVersion(value); return *this;}

    /**
     * The version of the output schema to use when exporting data. Must be V_1.
     */
    inline StorageClassAnalysisDataExport& WithOutputSchemaVersion(StorageClassAnalysisSchemaVersion&& value) { SetOutputSchemaVersion(std::move(value)); return *this;}


    /**
     * The place to store the data for an analysis.
     */
    inline const AnalyticsExportDestination& GetDestination() const{ return m_destination; }

    /**
     * The place to store the data for an analysis.
     */
    inline void SetDestination(const AnalyticsExportDestination& value) { m_destinationHasBeenSet = true; m_destination = value; }

    /**
     * The place to store the data for an analysis.
     */
    inline void SetDestination(AnalyticsExportDestination&& value) { m_destinationHasBeenSet = true; m_destination = std::move(value); }

    /**
     * The place to store the data for an analysis.
     */
    inline StorageClassAnalysisDataExport& WithDestination(const AnalyticsExportDestination& value) { SetDestination(value); return *this;}

    /**
     * The place to store the data for an analysis.
     */
    inline StorageClassAnalysisDataExport& WithDestination(AnalyticsExportDestination&& value) { SetDestination(std::move(value)); return *this;}

  private:

    StorageClassAnalysisSchemaVersion m_outputSchemaVersion;
    bool m_outputSchemaVersionHasBeenSet;

    AnalyticsExportDestination m_destination;
    bool m_destinationHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
