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

#include <aws/core/Core_EXPORTS.h>

#include <aws/core/utils/Array.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/external/json-cpp/json.h>

#include <utility>

namespace Aws
{
    namespace Utils
    {
        namespace Json
        {
            /**
             * Json Document tree object that supports parsing and serialization.
             */
            class AWS_CORE_API JsonValue
            {
            public:
                /**
                * Constructs empty json object
                */
                JsonValue();

                /**
                * Constructs a json object from a json string
                */

                JsonValue(const Aws::String& value);

                /**
                * Constructs a json object from a stream containing json
                */
                JsonValue(Aws::IStream& istream);

                /**
                * Copy Constructor
                */
                JsonValue(const JsonValue& value);

                /**
                * Move Constructor
                */
                JsonValue(JsonValue&& value);

                ~JsonValue();

                JsonValue& operator=(const JsonValue& other);

                JsonValue& operator=(JsonValue&& other);

                bool operator!=(const JsonValue& other)
                {
                    return m_value != other.m_value;
                }

                bool operator==(const JsonValue& other)
                {
                    return m_value == other.m_value;
                }

                /**
                * Gets a string from the top level of this node by it's key
                */
                Aws::String GetString(const Aws::String& key) const;
                Aws::String GetString(const char* key) const;

                /**
                * Adds a string to the top level of this node with key
                */
                JsonValue& WithString(const Aws::String& key, const Aws::String& value);
                JsonValue& WithString(const char* key, const Aws::String& value);

                /**
                * Causes the json node to be a string only (makes it a leaf node in token tree)
                */
                JsonValue& AsString(const Aws::String& value);

                /**
                * Returns the value of this node as a string as if it was a leaf node in the token tree
                */
                Aws::String AsString() const;

                /**
                * Gets a bool value from the top level of this node by its key.
                */
                bool GetBool(const Aws::String& key) const;
                bool GetBool(const char* key) const;

                /**
                * Adds a bool value with key to the top level of this node.
                */
                JsonValue& WithBool(const Aws::String& key, bool value);
                JsonValue& WithBool(const char* key, bool value);

                /**
                * Sets this node to be a bool (makes it a leaf node in the token tree)
                */
                JsonValue& AsBool(bool value);

                /**
                * Gets the value of this node as a bool
                */
                bool AsBool() const;

                /**
                * Gets the integer value at key on the top level of this node.
                */
                int GetInteger(const Aws::String& key) const;
                int GetInteger(const char* key) const;

                /**
                * Adds an integer value at key at the top level of this node.
                */
                JsonValue& WithInteger(const Aws::String& key, int value);
                JsonValue& WithInteger(const char* key, int value);

                /**
                * Causes this node to be an integer (becomes a leaf node in the token tree).
                */
                JsonValue& AsInteger(int value);

                /**
                * Gets the integer value from a leaf node.
                */
                int AsInteger() const;

                /**
                * Gets the 64 bit integer value at key from the top level of this node.
                */
                long long GetInt64(const Aws::String& key) const;
                long long GetInt64(const char* key) const;

                /**
                * Adds a 64 bit integer value at key to the top level of this node.
                */
                JsonValue& WithInt64(const Aws::String& key, long long value);
                JsonValue& WithInt64(const char* key, long long value);

                /**
                * Causes this node to be interpreted as a 64 bit integer (becomes treated like a leaf node).
                */
                JsonValue& AsInt64(long long value);

                /**
                * Gets the value of this node as a 64bit integer.
                */
                long long AsInt64() const;

                /**
                * Gets the value of a double at key at the top level of this node.
                */
                double GetDouble(const Aws::String& key) const;
                double GetDouble(const char* key) const;

                /**
                * Adds a double value at key at the top level of this node.
                */
                JsonValue& WithDouble(const Aws::String& key, double value);
                JsonValue& WithDouble(const char* key, double value);

                /**
                * Causes this node to be interpreted as a double value.
                */
                JsonValue& AsDouble(double value);

                /**
                * Gets the value of this node as a double.
                */
                double AsDouble() const;

                /**
                * Gets an array from the top level of this node at key.
                */
                Array<JsonValue> GetArray(const Aws::String& key) const;
                Array<JsonValue> GetArray(const char* key) const;

                /**
                * Adds an array of strings to the top level of this node at key.
                */
                JsonValue& WithArray(const Aws::String& key, const Array<Aws::String>& array);
                JsonValue& WithArray(const char* key, const Array<Aws::String>& array);

                /**
                * Adds an array of strings to the top level of this node at key. Array will be unusable after this call
                * only use if you intend this to be an r-value. Better yet, let the compiler make this decision
                * for you, but if you must.... std::move will do the trick.
                */
                JsonValue& WithArray(const Aws::String& key, Array<Aws::String>&& array);

                /**
                * Adds an array of arbitrary json objects to the top level of this node at key.
                */
                JsonValue& WithArray(const Aws::String& key, const Array<JsonValue>& array);

                /**
                * Adds an array of arbitrary json objects to the top level of this node at key. Array will be unusable after this call
                * only use if you intend this to be an r-value. Better yet, let the compiler make this decision
                * for you, but if you must.... std::move will do the trick.
                */
                JsonValue& WithArray(const Aws::String& key, Array<JsonValue>&& array);

                /**
                * Causes this node to be interpreted as an array.
                */
                JsonValue& AsArray(const Array<JsonValue>& array);

                /**
                * Causes this node to be interpreted as an array using move semantics on array.
                */
                JsonValue& AsArray(Array<JsonValue>&& array);

                /**
                * Interprets this node as an array and returns a copy of it's values.
                */
                Array<JsonValue> AsArray() const;

                /**
                * Gets a json object from the top level of this node at key.
                */
                JsonValue GetObject(const char* key) const;
                JsonValue GetObject(const Aws::String& key) const;

                /**
                * Adds a json object to the top level of this node at key.
                */
                JsonValue& WithObject(const Aws::String& key, const JsonValue& value);
                JsonValue& WithObject(const char* key, const JsonValue& value);

                /**
                * Adds a json object to the top level of this node at key using move semantics.
                */
                JsonValue& WithObject(const Aws::String& key, const JsonValue&& value);
                JsonValue& WithObject(const char* key, const JsonValue&& value);

                /**
                * Causes this node to be interpreted as another json object
                */
                JsonValue& AsObject(const JsonValue& value);

                /**
                * Causes this node to be interpreted as another json object using move semantics.
                */
                JsonValue& AsObject(JsonValue&& value);

                /**
                * Gets the value of this node as a json object.
                */
                JsonValue AsObject() const;

                /**
                * Reads all json objects at the top level of this node (does not traverse the tree any further)
                * along with their keys.
                */
                Aws::Map<Aws::String, JsonValue> GetAllObjects() const;

                /**
                * Whether or not a value exists at the current node level at a given key.
                *
                * Returns true if a values has been found, false otherwise.
                */
                bool ValueExists(const char* key) const;
                bool ValueExists(const Aws::String& key) const;

                /**
                * Writes the entire json object tree without whitespace characters starting at the current level to a string and
                * returns it.
                */
                Aws::String WriteCompact(bool treatAsObject = true) const;

                /**
                * Writes the entire json object tree to ostream without whitespace characters at the current level.
                */
                void WriteCompact(Aws::OStream& ostream, bool treatAsObject = true) const;

                /**
                * Writes the entire json object tree starting at the current level to a string and
                * returns it.
                */
                Aws::String WriteReadable(bool treatAsObject = true) const;

                /**
                * Writes the entire json object tree to ostream at the current level.
                */
                void WriteReadable(Aws::OStream& ostream, bool treatAsObject = true) const;

                /**
                 * Returns true if the last parse request was successful. If this returns false,
                 * you can call GetErrorMessage() to find the cause.
                 */
                inline bool WasParseSuccessful() const
                {
                    return m_wasParseSuccessful;
                }

                /**
                 * Returns the last error message from a failed parse attempt. Returns empty string if no error.
                 */
                inline const Aws::String& GetErrorMessage() const
                {
                    return m_errorMessage;
                }
                /**
                 * Appends a json object as a child to the end of this object
                 */
                void AppendValue(const JsonValue& value);

                bool IsObject() const;
                bool IsBool() const;
                bool IsString() const;
                bool IsIntegerType() const;
                bool IsFloatingPointType() const;
                bool IsListType() const;

                Aws::External::Json::Value& ModifyRawValue() { return m_value; }

            private:
                JsonValue(const Aws::External::Json::Value& value);

                JsonValue& operator=(Aws::External::Json::Value& other);

                mutable Aws::External::Json::Value m_value;
                bool m_wasParseSuccessful;
                Aws::String m_errorMessage;
            };

        } // namespace Json
    } // namespace Utils
} // namespace Aws

