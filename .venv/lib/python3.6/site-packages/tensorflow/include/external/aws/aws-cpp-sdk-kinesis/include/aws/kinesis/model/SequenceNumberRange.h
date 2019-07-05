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
#include <aws/kinesis/Kinesis_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <utility>

namespace Aws
{
namespace Utils
{
namespace Json
{
  class JsonValue;
} // namespace Json
} // namespace Utils
namespace Kinesis
{
namespace Model
{

  /**
   * <p>The range of possible sequence numbers for the shard.</p><p><h3>See
   * Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/SequenceNumberRange">AWS
   * API Reference</a></p>
   */
  class AWS_KINESIS_API SequenceNumberRange
  {
  public:
    SequenceNumberRange();
    SequenceNumberRange(const Aws::Utils::Json::JsonValue& jsonValue);
    SequenceNumberRange& operator=(const Aws::Utils::Json::JsonValue& jsonValue);
    Aws::Utils::Json::JsonValue Jsonize() const;


    /**
     * <p>The starting sequence number for the range.</p>
     */
    inline const Aws::String& GetStartingSequenceNumber() const{ return m_startingSequenceNumber; }

    /**
     * <p>The starting sequence number for the range.</p>
     */
    inline void SetStartingSequenceNumber(const Aws::String& value) { m_startingSequenceNumberHasBeenSet = true; m_startingSequenceNumber = value; }

    /**
     * <p>The starting sequence number for the range.</p>
     */
    inline void SetStartingSequenceNumber(Aws::String&& value) { m_startingSequenceNumberHasBeenSet = true; m_startingSequenceNumber = std::move(value); }

    /**
     * <p>The starting sequence number for the range.</p>
     */
    inline void SetStartingSequenceNumber(const char* value) { m_startingSequenceNumberHasBeenSet = true; m_startingSequenceNumber.assign(value); }

    /**
     * <p>The starting sequence number for the range.</p>
     */
    inline SequenceNumberRange& WithStartingSequenceNumber(const Aws::String& value) { SetStartingSequenceNumber(value); return *this;}

    /**
     * <p>The starting sequence number for the range.</p>
     */
    inline SequenceNumberRange& WithStartingSequenceNumber(Aws::String&& value) { SetStartingSequenceNumber(std::move(value)); return *this;}

    /**
     * <p>The starting sequence number for the range.</p>
     */
    inline SequenceNumberRange& WithStartingSequenceNumber(const char* value) { SetStartingSequenceNumber(value); return *this;}


    /**
     * <p>The ending sequence number for the range. Shards that are in the OPEN state
     * have an ending sequence number of <code>null</code>.</p>
     */
    inline const Aws::String& GetEndingSequenceNumber() const{ return m_endingSequenceNumber; }

    /**
     * <p>The ending sequence number for the range. Shards that are in the OPEN state
     * have an ending sequence number of <code>null</code>.</p>
     */
    inline void SetEndingSequenceNumber(const Aws::String& value) { m_endingSequenceNumberHasBeenSet = true; m_endingSequenceNumber = value; }

    /**
     * <p>The ending sequence number for the range. Shards that are in the OPEN state
     * have an ending sequence number of <code>null</code>.</p>
     */
    inline void SetEndingSequenceNumber(Aws::String&& value) { m_endingSequenceNumberHasBeenSet = true; m_endingSequenceNumber = std::move(value); }

    /**
     * <p>The ending sequence number for the range. Shards that are in the OPEN state
     * have an ending sequence number of <code>null</code>.</p>
     */
    inline void SetEndingSequenceNumber(const char* value) { m_endingSequenceNumberHasBeenSet = true; m_endingSequenceNumber.assign(value); }

    /**
     * <p>The ending sequence number for the range. Shards that are in the OPEN state
     * have an ending sequence number of <code>null</code>.</p>
     */
    inline SequenceNumberRange& WithEndingSequenceNumber(const Aws::String& value) { SetEndingSequenceNumber(value); return *this;}

    /**
     * <p>The ending sequence number for the range. Shards that are in the OPEN state
     * have an ending sequence number of <code>null</code>.</p>
     */
    inline SequenceNumberRange& WithEndingSequenceNumber(Aws::String&& value) { SetEndingSequenceNumber(std::move(value)); return *this;}

    /**
     * <p>The ending sequence number for the range. Shards that are in the OPEN state
     * have an ending sequence number of <code>null</code>.</p>
     */
    inline SequenceNumberRange& WithEndingSequenceNumber(const char* value) { SetEndingSequenceNumber(value); return *this;}

  private:

    Aws::String m_startingSequenceNumber;
    bool m_startingSequenceNumberHasBeenSet;

    Aws::String m_endingSequenceNumber;
    bool m_endingSequenceNumberHasBeenSet;
  };

} // namespace Model
} // namespace Kinesis
} // namespace Aws
