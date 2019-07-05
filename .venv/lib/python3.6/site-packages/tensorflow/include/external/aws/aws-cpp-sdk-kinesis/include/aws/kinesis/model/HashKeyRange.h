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
   * <p>The range of possible hash key values for the shard, which is a set of
   * ordered contiguous positive integers.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/HashKeyRange">AWS
   * API Reference</a></p>
   */
  class AWS_KINESIS_API HashKeyRange
  {
  public:
    HashKeyRange();
    HashKeyRange(const Aws::Utils::Json::JsonValue& jsonValue);
    HashKeyRange& operator=(const Aws::Utils::Json::JsonValue& jsonValue);
    Aws::Utils::Json::JsonValue Jsonize() const;


    /**
     * <p>The starting hash key of the hash key range.</p>
     */
    inline const Aws::String& GetStartingHashKey() const{ return m_startingHashKey; }

    /**
     * <p>The starting hash key of the hash key range.</p>
     */
    inline void SetStartingHashKey(const Aws::String& value) { m_startingHashKeyHasBeenSet = true; m_startingHashKey = value; }

    /**
     * <p>The starting hash key of the hash key range.</p>
     */
    inline void SetStartingHashKey(Aws::String&& value) { m_startingHashKeyHasBeenSet = true; m_startingHashKey = std::move(value); }

    /**
     * <p>The starting hash key of the hash key range.</p>
     */
    inline void SetStartingHashKey(const char* value) { m_startingHashKeyHasBeenSet = true; m_startingHashKey.assign(value); }

    /**
     * <p>The starting hash key of the hash key range.</p>
     */
    inline HashKeyRange& WithStartingHashKey(const Aws::String& value) { SetStartingHashKey(value); return *this;}

    /**
     * <p>The starting hash key of the hash key range.</p>
     */
    inline HashKeyRange& WithStartingHashKey(Aws::String&& value) { SetStartingHashKey(std::move(value)); return *this;}

    /**
     * <p>The starting hash key of the hash key range.</p>
     */
    inline HashKeyRange& WithStartingHashKey(const char* value) { SetStartingHashKey(value); return *this;}


    /**
     * <p>The ending hash key of the hash key range.</p>
     */
    inline const Aws::String& GetEndingHashKey() const{ return m_endingHashKey; }

    /**
     * <p>The ending hash key of the hash key range.</p>
     */
    inline void SetEndingHashKey(const Aws::String& value) { m_endingHashKeyHasBeenSet = true; m_endingHashKey = value; }

    /**
     * <p>The ending hash key of the hash key range.</p>
     */
    inline void SetEndingHashKey(Aws::String&& value) { m_endingHashKeyHasBeenSet = true; m_endingHashKey = std::move(value); }

    /**
     * <p>The ending hash key of the hash key range.</p>
     */
    inline void SetEndingHashKey(const char* value) { m_endingHashKeyHasBeenSet = true; m_endingHashKey.assign(value); }

    /**
     * <p>The ending hash key of the hash key range.</p>
     */
    inline HashKeyRange& WithEndingHashKey(const Aws::String& value) { SetEndingHashKey(value); return *this;}

    /**
     * <p>The ending hash key of the hash key range.</p>
     */
    inline HashKeyRange& WithEndingHashKey(Aws::String&& value) { SetEndingHashKey(std::move(value)); return *this;}

    /**
     * <p>The ending hash key of the hash key range.</p>
     */
    inline HashKeyRange& WithEndingHashKey(const char* value) { SetEndingHashKey(value); return *this;}

  private:

    Aws::String m_startingHashKey;
    bool m_startingHashKeyHasBeenSet;

    Aws::String m_endingHashKey;
    bool m_endingHashKeyHasBeenSet;
  };

} // namespace Model
} // namespace Kinesis
} // namespace Aws
