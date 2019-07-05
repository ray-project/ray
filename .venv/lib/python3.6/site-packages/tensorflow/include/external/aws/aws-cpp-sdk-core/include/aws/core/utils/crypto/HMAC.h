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

#include <aws/core/Core_EXPORTS.h>

#include <aws/core/utils/Array.h>
#include <aws/core/utils/crypto/HashResult.h>

namespace Aws
{
    namespace Utils
    {
        namespace Crypto
        {
            /**
             * Interface for HMAC hash providers. To implement an HMAC provider, inherit from this class and override Calculate.
             */
            class AWS_CORE_API HMAC
            {
            public:
                HMAC() {};
                virtual ~HMAC() {};

                /**
                * Calculates an HMAC digest
                */
                virtual HashResult Calculate(const Aws::Utils::ByteBuffer& toSign, const Aws::Utils::ByteBuffer& secret) = 0;

            };

            /**
             * Simple abstract factory interface. Subclass this and create a factory if you want to control
             * how HMAC Hash objects are created.
             */
            class AWS_CORE_API HMACFactory
            {
            public:
                virtual ~HMACFactory() {}

                /**
                * Factory method. Returns hmac hash implementation.
                */
                virtual std::shared_ptr<HMAC> CreateImplementation() const = 0;

                /**
                 * Opportunity to make any static initialization calls you need to make.
                 * Will only be called once.
                 */
                virtual void InitStaticState() {}

                /**
                 * Opportunity to make any static cleanup calls you need to make.
                 * will only be called at the end of the application.
                 */
                virtual void CleanupStaticState() {}
            };

        } // namespace Crypto
    } // namespace Utils
} // namespace Aws

