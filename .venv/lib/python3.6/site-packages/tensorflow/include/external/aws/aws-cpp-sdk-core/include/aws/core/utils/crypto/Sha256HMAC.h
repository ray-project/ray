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

#include <aws/core/utils/crypto/HMAC.h>
#include <aws/core/utils/memory/AWSMemory.h>

namespace Aws
{
    namespace Utils
    {
        namespace Crypto
        {
            /**
             * Sha256 HMAC implementation
             */
            class AWS_CORE_API Sha256HMAC : public HMAC
            {
            public:
                /**
                 * initializes platform specific libs.
                 */
                Sha256HMAC();
                virtual ~Sha256HMAC();

                /**
                * Calculates a SHA256 HMAC digest (not hex encoded)
                */
                virtual HashResult Calculate(const Aws::Utils::ByteBuffer& toSign, const Aws::Utils::ByteBuffer& secret) override;

            private:

                std::shared_ptr< HMAC > m_hmacImpl;
            };

        } // namespace Sha256
    } // namespace Utils
} // namespace Aws

