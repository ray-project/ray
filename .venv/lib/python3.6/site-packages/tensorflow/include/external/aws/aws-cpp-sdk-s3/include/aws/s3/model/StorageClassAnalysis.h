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
#include <aws/s3/model/StorageClassAnalysisDataExport.h>
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

  class AWS_S3_API StorageClassAnalysis
  {
  public:
    StorageClassAnalysis();
    StorageClassAnalysis(const Aws::Utils::Xml::XmlNode& xmlNode);
    StorageClassAnalysis& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * A container used to describe how data related to the storage class analysis
     * should be exported.
     */
    inline const StorageClassAnalysisDataExport& GetDataExport() const{ return m_dataExport; }

    /**
     * A container used to describe how data related to the storage class analysis
     * should be exported.
     */
    inline void SetDataExport(const StorageClassAnalysisDataExport& value) { m_dataExportHasBeenSet = true; m_dataExport = value; }

    /**
     * A container used to describe how data related to the storage class analysis
     * should be exported.
     */
    inline void SetDataExport(StorageClassAnalysisDataExport&& value) { m_dataExportHasBeenSet = true; m_dataExport = std::move(value); }

    /**
     * A container used to describe how data related to the storage class analysis
     * should be exported.
     */
    inline StorageClassAnalysis& WithDataExport(const StorageClassAnalysisDataExport& value) { SetDataExport(value); return *this;}

    /**
     * A container used to describe how data related to the storage class analysis
     * should be exported.
     */
    inline StorageClassAnalysis& WithDataExport(StorageClassAnalysisDataExport&& value) { SetDataExport(std::move(value)); return *this;}

  private:

    StorageClassAnalysisDataExport m_dataExport;
    bool m_dataExportHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
