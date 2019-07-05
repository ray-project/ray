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
   * <p>Metadata assigned to the stream, consisting of a key-value
   * pair.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/Tag">AWS API
   * Reference</a></p>
   */
  class AWS_KINESIS_API Tag
  {
  public:
    Tag();
    Tag(const Aws::Utils::Json::JsonValue& jsonValue);
    Tag& operator=(const Aws::Utils::Json::JsonValue& jsonValue);
    Aws::Utils::Json::JsonValue Jsonize() const;


    /**
     * <p>A unique identifier for the tag. Maximum length: 128 characters. Valid
     * characters: Unicode letters, digits, white space, _ . / = + - % @</p>
     */
    inline const Aws::String& GetKey() const{ return m_key; }

    /**
     * <p>A unique identifier for the tag. Maximum length: 128 characters. Valid
     * characters: Unicode letters, digits, white space, _ . / = + - % @</p>
     */
    inline void SetKey(const Aws::String& value) { m_keyHasBeenSet = true; m_key = value; }

    /**
     * <p>A unique identifier for the tag. Maximum length: 128 characters. Valid
     * characters: Unicode letters, digits, white space, _ . / = + - % @</p>
     */
    inline void SetKey(Aws::String&& value) { m_keyHasBeenSet = true; m_key = std::move(value); }

    /**
     * <p>A unique identifier for the tag. Maximum length: 128 characters. Valid
     * characters: Unicode letters, digits, white space, _ . / = + - % @</p>
     */
    inline void SetKey(const char* value) { m_keyHasBeenSet = true; m_key.assign(value); }

    /**
     * <p>A unique identifier for the tag. Maximum length: 128 characters. Valid
     * characters: Unicode letters, digits, white space, _ . / = + - % @</p>
     */
    inline Tag& WithKey(const Aws::String& value) { SetKey(value); return *this;}

    /**
     * <p>A unique identifier for the tag. Maximum length: 128 characters. Valid
     * characters: Unicode letters, digits, white space, _ . / = + - % @</p>
     */
    inline Tag& WithKey(Aws::String&& value) { SetKey(std::move(value)); return *this;}

    /**
     * <p>A unique identifier for the tag. Maximum length: 128 characters. Valid
     * characters: Unicode letters, digits, white space, _ . / = + - % @</p>
     */
    inline Tag& WithKey(const char* value) { SetKey(value); return *this;}


    /**
     * <p>An optional string, typically used to describe or define the tag. Maximum
     * length: 256 characters. Valid characters: Unicode letters, digits, white space,
     * _ . / = + - % @</p>
     */
    inline const Aws::String& GetValue() const{ return m_value; }

    /**
     * <p>An optional string, typically used to describe or define the tag. Maximum
     * length: 256 characters. Valid characters: Unicode letters, digits, white space,
     * _ . / = + - % @</p>
     */
    inline void SetValue(const Aws::String& value) { m_valueHasBeenSet = true; m_value = value; }

    /**
     * <p>An optional string, typically used to describe or define the tag. Maximum
     * length: 256 characters. Valid characters: Unicode letters, digits, white space,
     * _ . / = + - % @</p>
     */
    inline void SetValue(Aws::String&& value) { m_valueHasBeenSet = true; m_value = std::move(value); }

    /**
     * <p>An optional string, typically used to describe or define the tag. Maximum
     * length: 256 characters. Valid characters: Unicode letters, digits, white space,
     * _ . / = + - % @</p>
     */
    inline void SetValue(const char* value) { m_valueHasBeenSet = true; m_value.assign(value); }

    /**
     * <p>An optional string, typically used to describe or define the tag. Maximum
     * length: 256 characters. Valid characters: Unicode letters, digits, white space,
     * _ . / = + - % @</p>
     */
    inline Tag& WithValue(const Aws::String& value) { SetValue(value); return *this;}

    /**
     * <p>An optional string, typically used to describe or define the tag. Maximum
     * length: 256 characters. Valid characters: Unicode letters, digits, white space,
     * _ . / = + - % @</p>
     */
    inline Tag& WithValue(Aws::String&& value) { SetValue(std::move(value)); return *this;}

    /**
     * <p>An optional string, typically used to describe or define the tag. Maximum
     * length: 256 characters. Valid characters: Unicode letters, digits, white space,
     * _ . / = + - % @</p>
     */
    inline Tag& WithValue(const char* value) { SetValue(value); return *this;}

  private:

    Aws::String m_key;
    bool m_keyHasBeenSet;

    Aws::String m_value;
    bool m_valueHasBeenSet;
  };

} // namespace Model
} // namespace Kinesis
} // namespace Aws
