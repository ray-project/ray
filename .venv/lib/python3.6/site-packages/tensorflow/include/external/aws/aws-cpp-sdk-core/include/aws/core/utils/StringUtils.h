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

#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>



namespace Aws
{
    namespace Utils
    {
        /**
        * All the things the c++ stdlib is missing for string operations that I needed.
        */
        class AWS_CORE_API StringUtils
        {
        public:
            static void Replace(Aws::String& s, const char* search, const char* replace);


            /**
            * Converts a string to lower case.
            */
            static Aws::String ToLower(const char* source);


            /**
            * Converts a string to upper case.
            */
            static Aws::String ToUpper(const char* source);


            /**
            * Does a caseless comparison of two strings.
            */
            static bool CaselessCompare(const char* value1, const char* value2);


            /**
            * URL encodes a string (uses %20 not + for spaces).
            */
            static Aws::String URLEncode(const char* unsafe);

            /**
            * Http Clients tend to escape some characters but not all. Escaping all of them causes problems, because the client
            * will also try to escape them.
            * So this only escapes non-ascii characters and the + character
            */
            static Aws::String UTF8Escape(const char* unicodeString, const char* delimiter);

            /**
            * URL encodes a double (if it ends up going to scientific notation) otherwise it just returns it as a string.
            */
            static Aws::String URLEncode(double unsafe);


            /**
            * Decodes a URL encoded string (will handle both encoding schemes for spaces).
            */
            static Aws::String URLDecode(const char* safe);


            /**
            * Splits a string on a delimiter (empty items are excluded).
            */
            static Aws::Vector<Aws::String> Split(const Aws::String& toSplit, char splitOn);


            /**
            * Splits a string on new line characters.
            */
            static Aws::Vector<Aws::String> SplitOnLine(const Aws::String& toSplit);


            /** static Aws::Vector<Aws::String> SplitOnRegex(Aws::String regex);
             *  trim from start
             */
            static Aws::String LTrim(const char* source);


            /**
             * trim from end
             */
            static Aws::String RTrim(const char* source);

            /**
             * trim from both ends
             */
            static Aws::String Trim(const char* source);


            /**
             * convert to int 64
             */
            static long long ConvertToInt64(const char* source);


            /**
             * convert to int 32
             */
            static long ConvertToInt32(const char* source);


            /** 
             * convert to bool
             */
            static bool ConvertToBool(const char* source);


            /**
             * convert to double
             */
            static double ConvertToDouble(const char* source);


#ifdef _WIN32
            /**
            * Converts a string to wstring.
            */
            static Aws::WString ToWString(const char* source);

            /**
            * Converts a wstring to string.
            */
            static Aws::String FromWString(const wchar_t* source);
#endif

            /**
             * not all platforms (Android) have std::to_string
             */ 
            template< typename T >
            static Aws::String to_string(T value)
            {
                Aws::OStringStream os;
                os << value;
                return os.str();
            }

        };


    } // namespace Utils
} // namespace Aws


