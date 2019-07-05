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
#include <aws/s3/model/CSVOutput.h>
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
   * Describes how results of the Select job are serialized.<p><h3>See Also:</h3>  
   * <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/OutputSerialization">AWS
   * API Reference</a></p>
   */
  class AWS_S3_API OutputSerialization
  {
  public:
    OutputSerialization();
    OutputSerialization(const Aws::Utils::Xml::XmlNode& xmlNode);
    OutputSerialization& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * Describes the serialization of CSV-encoded Select results.
     */
    inline const CSVOutput& GetCSV() const{ return m_cSV; }

    /**
     * Describes the serialization of CSV-encoded Select results.
     */
    inline void SetCSV(const CSVOutput& value) { m_cSVHasBeenSet = true; m_cSV = value; }

    /**
     * Describes the serialization of CSV-encoded Select results.
     */
    inline void SetCSV(CSVOutput&& value) { m_cSVHasBeenSet = true; m_cSV = std::move(value); }

    /**
     * Describes the serialization of CSV-encoded Select results.
     */
    inline OutputSerialization& WithCSV(const CSVOutput& value) { SetCSV(value); return *this;}

    /**
     * Describes the serialization of CSV-encoded Select results.
     */
    inline OutputSerialization& WithCSV(CSVOutput&& value) { SetCSV(std::move(value)); return *this;}

  private:

    CSVOutput m_cSV;
    bool m_cSVHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
