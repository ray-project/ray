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

  /*
  * Interface for Sha256 encryptor and hmac
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

#include <aws/core/utils/crypto/Hash.h>
#include <aws/core/utils/Outcome.h>

namespace Aws
{
    namespace Utils
    {
        namespace Crypto
        {
            class WindowsHashImpl;

            /**
             * Md5 hash implementation
             */
            class AWS_CORE_API MD5 : public Hash
            {
            public:
                /**
                 * Initializes platform crypo libs for md5
                 */
                MD5();
                virtual ~MD5();

                /**
                * Calculates an MD5 hash
                */
                virtual HashResult Calculate(const Aws::String& str) override;

                /**
                * Calculates a Hash digest on a stream (the entire stream is read)
                */
                virtual HashResult Calculate(Aws::IStream& stream) override;

            private:

                std::shared_ptr<Hash> m_hashImpl;
            };

        } // namespace Crypto
    } // namespace Utils
} // namespace Aws

