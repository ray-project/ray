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
#include <aws/core/utils/Array.h>

namespace Aws
{
    namespace Utils
    {
        static const size_t UUID_BINARY_SIZE = 0x10;
        static const size_t UUID_STR_SIZE = 0x24;

        /**
         * Class encapsulating a UUID. This is platform dependent. The method you are most likely interested in is RandomUUID().
         */
        class AWS_CORE_API UUID
        {
        public:
            /**
             * Parses a GUID string into the raw data.
             */
            UUID(const Aws::String&);
            /**
             * Sets the raw uuid data
             */
            UUID(const unsigned char uuid[UUID_BINARY_SIZE]);

            /**
             * Returns the current UUID as a GUID string
             */
            operator Aws::String();
            /**
             * Returns a copy of the raw uuid
             */
            inline operator ByteBuffer() { return ByteBuffer(m_uuid, sizeof(m_uuid)); }

            /**
             * Generates a UUID. It will always try to prefer a random implementation from the entropy source on the machine. If none, is available, it will
             * fallback to the mac address and timestamp implementation.
             */
            static UUID RandomUUID();

        private:
            unsigned char m_uuid[UUID_BINARY_SIZE];
        };
    }
}