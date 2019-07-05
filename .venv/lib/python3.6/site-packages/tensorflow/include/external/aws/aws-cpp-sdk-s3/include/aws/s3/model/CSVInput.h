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
#include <aws/s3/model/FileHeaderInfo.h>
#include <aws/core/utils/memory/stl/AWSString.h>
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
   * Describes how a CSV-formatted input object is formatted.<p><h3>See Also:</h3>  
   * <a href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/CSVInput">AWS API
   * Reference</a></p>
   */
  class AWS_S3_API CSVInput
  {
  public:
    CSVInput();
    CSVInput(const Aws::Utils::Xml::XmlNode& xmlNode);
    CSVInput& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * Describes the first line of input. Valid values: None, Ignore, Use.
     */
    inline const FileHeaderInfo& GetFileHeaderInfo() const{ return m_fileHeaderInfo; }

    /**
     * Describes the first line of input. Valid values: None, Ignore, Use.
     */
    inline void SetFileHeaderInfo(const FileHeaderInfo& value) { m_fileHeaderInfoHasBeenSet = true; m_fileHeaderInfo = value; }

    /**
     * Describes the first line of input. Valid values: None, Ignore, Use.
     */
    inline void SetFileHeaderInfo(FileHeaderInfo&& value) { m_fileHeaderInfoHasBeenSet = true; m_fileHeaderInfo = std::move(value); }

    /**
     * Describes the first line of input. Valid values: None, Ignore, Use.
     */
    inline CSVInput& WithFileHeaderInfo(const FileHeaderInfo& value) { SetFileHeaderInfo(value); return *this;}

    /**
     * Describes the first line of input. Valid values: None, Ignore, Use.
     */
    inline CSVInput& WithFileHeaderInfo(FileHeaderInfo&& value) { SetFileHeaderInfo(std::move(value)); return *this;}


    /**
     * Single character used to indicate a row should be ignored when present at the
     * start of a row.
     */
    inline const Aws::String& GetComments() const{ return m_comments; }

    /**
     * Single character used to indicate a row should be ignored when present at the
     * start of a row.
     */
    inline void SetComments(const Aws::String& value) { m_commentsHasBeenSet = true; m_comments = value; }

    /**
     * Single character used to indicate a row should be ignored when present at the
     * start of a row.
     */
    inline void SetComments(Aws::String&& value) { m_commentsHasBeenSet = true; m_comments = std::move(value); }

    /**
     * Single character used to indicate a row should be ignored when present at the
     * start of a row.
     */
    inline void SetComments(const char* value) { m_commentsHasBeenSet = true; m_comments.assign(value); }

    /**
     * Single character used to indicate a row should be ignored when present at the
     * start of a row.
     */
    inline CSVInput& WithComments(const Aws::String& value) { SetComments(value); return *this;}

    /**
     * Single character used to indicate a row should be ignored when present at the
     * start of a row.
     */
    inline CSVInput& WithComments(Aws::String&& value) { SetComments(std::move(value)); return *this;}

    /**
     * Single character used to indicate a row should be ignored when present at the
     * start of a row.
     */
    inline CSVInput& WithComments(const char* value) { SetComments(value); return *this;}


    /**
     * Single character used for escaping the quote character inside an already escaped
     * value.
     */
    inline const Aws::String& GetQuoteEscapeCharacter() const{ return m_quoteEscapeCharacter; }

    /**
     * Single character used for escaping the quote character inside an already escaped
     * value.
     */
    inline void SetQuoteEscapeCharacter(const Aws::String& value) { m_quoteEscapeCharacterHasBeenSet = true; m_quoteEscapeCharacter = value; }

    /**
     * Single character used for escaping the quote character inside an already escaped
     * value.
     */
    inline void SetQuoteEscapeCharacter(Aws::String&& value) { m_quoteEscapeCharacterHasBeenSet = true; m_quoteEscapeCharacter = std::move(value); }

    /**
     * Single character used for escaping the quote character inside an already escaped
     * value.
     */
    inline void SetQuoteEscapeCharacter(const char* value) { m_quoteEscapeCharacterHasBeenSet = true; m_quoteEscapeCharacter.assign(value); }

    /**
     * Single character used for escaping the quote character inside an already escaped
     * value.
     */
    inline CSVInput& WithQuoteEscapeCharacter(const Aws::String& value) { SetQuoteEscapeCharacter(value); return *this;}

    /**
     * Single character used for escaping the quote character inside an already escaped
     * value.
     */
    inline CSVInput& WithQuoteEscapeCharacter(Aws::String&& value) { SetQuoteEscapeCharacter(std::move(value)); return *this;}

    /**
     * Single character used for escaping the quote character inside an already escaped
     * value.
     */
    inline CSVInput& WithQuoteEscapeCharacter(const char* value) { SetQuoteEscapeCharacter(value); return *this;}


    /**
     * Value used to separate individual records.
     */
    inline const Aws::String& GetRecordDelimiter() const{ return m_recordDelimiter; }

    /**
     * Value used to separate individual records.
     */
    inline void SetRecordDelimiter(const Aws::String& value) { m_recordDelimiterHasBeenSet = true; m_recordDelimiter = value; }

    /**
     * Value used to separate individual records.
     */
    inline void SetRecordDelimiter(Aws::String&& value) { m_recordDelimiterHasBeenSet = true; m_recordDelimiter = std::move(value); }

    /**
     * Value used to separate individual records.
     */
    inline void SetRecordDelimiter(const char* value) { m_recordDelimiterHasBeenSet = true; m_recordDelimiter.assign(value); }

    /**
     * Value used to separate individual records.
     */
    inline CSVInput& WithRecordDelimiter(const Aws::String& value) { SetRecordDelimiter(value); return *this;}

    /**
     * Value used to separate individual records.
     */
    inline CSVInput& WithRecordDelimiter(Aws::String&& value) { SetRecordDelimiter(std::move(value)); return *this;}

    /**
     * Value used to separate individual records.
     */
    inline CSVInput& WithRecordDelimiter(const char* value) { SetRecordDelimiter(value); return *this;}


    /**
     * Value used to separate individual fields in a record.
     */
    inline const Aws::String& GetFieldDelimiter() const{ return m_fieldDelimiter; }

    /**
     * Value used to separate individual fields in a record.
     */
    inline void SetFieldDelimiter(const Aws::String& value) { m_fieldDelimiterHasBeenSet = true; m_fieldDelimiter = value; }

    /**
     * Value used to separate individual fields in a record.
     */
    inline void SetFieldDelimiter(Aws::String&& value) { m_fieldDelimiterHasBeenSet = true; m_fieldDelimiter = std::move(value); }

    /**
     * Value used to separate individual fields in a record.
     */
    inline void SetFieldDelimiter(const char* value) { m_fieldDelimiterHasBeenSet = true; m_fieldDelimiter.assign(value); }

    /**
     * Value used to separate individual fields in a record.
     */
    inline CSVInput& WithFieldDelimiter(const Aws::String& value) { SetFieldDelimiter(value); return *this;}

    /**
     * Value used to separate individual fields in a record.
     */
    inline CSVInput& WithFieldDelimiter(Aws::String&& value) { SetFieldDelimiter(std::move(value)); return *this;}

    /**
     * Value used to separate individual fields in a record.
     */
    inline CSVInput& WithFieldDelimiter(const char* value) { SetFieldDelimiter(value); return *this;}


    /**
     * Value used for escaping where the field delimiter is part of the value.
     */
    inline const Aws::String& GetQuoteCharacter() const{ return m_quoteCharacter; }

    /**
     * Value used for escaping where the field delimiter is part of the value.
     */
    inline void SetQuoteCharacter(const Aws::String& value) { m_quoteCharacterHasBeenSet = true; m_quoteCharacter = value; }

    /**
     * Value used for escaping where the field delimiter is part of the value.
     */
    inline void SetQuoteCharacter(Aws::String&& value) { m_quoteCharacterHasBeenSet = true; m_quoteCharacter = std::move(value); }

    /**
     * Value used for escaping where the field delimiter is part of the value.
     */
    inline void SetQuoteCharacter(const char* value) { m_quoteCharacterHasBeenSet = true; m_quoteCharacter.assign(value); }

    /**
     * Value used for escaping where the field delimiter is part of the value.
     */
    inline CSVInput& WithQuoteCharacter(const Aws::String& value) { SetQuoteCharacter(value); return *this;}

    /**
     * Value used for escaping where the field delimiter is part of the value.
     */
    inline CSVInput& WithQuoteCharacter(Aws::String&& value) { SetQuoteCharacter(std::move(value)); return *this;}

    /**
     * Value used for escaping where the field delimiter is part of the value.
     */
    inline CSVInput& WithQuoteCharacter(const char* value) { SetQuoteCharacter(value); return *this;}

  private:

    FileHeaderInfo m_fileHeaderInfo;
    bool m_fileHeaderInfoHasBeenSet;

    Aws::String m_comments;
    bool m_commentsHasBeenSet;

    Aws::String m_quoteEscapeCharacter;
    bool m_quoteEscapeCharacterHasBeenSet;

    Aws::String m_recordDelimiter;
    bool m_recordDelimiterHasBeenSet;

    Aws::String m_fieldDelimiter;
    bool m_fieldDelimiterHasBeenSet;

    Aws::String m_quoteCharacter;
    bool m_quoteCharacterHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
