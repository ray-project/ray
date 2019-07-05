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

#ifdef __APPLE__

#ifdef __clang__
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
#endif // __clang__

#ifdef __GNUC__
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif // __GNUC__

#endif // __APPLE__

#include <aws/core/Core_EXPORTS.h>
#include <aws/core/utils/Array.h>
#include <aws/core/utils/memory/stl/AWSString.h>

namespace Aws
{
    namespace Utils
    {
        namespace Base64
        {

            /**
             * interface for platform specific Base64 encoding/decoding.
             */
            class AWS_CORE_API Base64
            {
            public:
                Base64(const char *encodingTable = nullptr);

                /**
                * Encode a byte buffer into a base64 stream.
                *
                * throws Base64Exception if encoding fails.
                */
                Aws::String Encode(const ByteBuffer&) const;

                /**
                * Decode a base64 string into a byte buffer.
                */
                ByteBuffer Decode(const Aws::String&) const;

                /**
                * Calculates the required length of a base64 buffer after decoding the
                * input string.
                */
                static size_t CalculateBase64DecodedLength(const Aws::String& b64input);
                /**
                * Calculates the length of an encoded base64 string based on the buffer being encoded
                */
                static size_t CalculateBase64EncodedLength(const ByteBuffer& buffer);

            private:
                char m_mimeBase64EncodingTable[64];
                uint8_t m_mimeBase64DecodingTable[256];

            };

        } // namespace Base64
    } // namespace Utils
} // namespace Aws

