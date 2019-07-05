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
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <memory>

namespace Aws
{
    namespace Auth
    {
        /**
         * Abstract class for providing chains of credentials providers. When a credentials provider in the chain returns empty credentials,
         * We go on to the next provider until we have either exhausted the installed providers in the chain or something returns non-empty credentials.
         */
        class AWS_CORE_API AWSCredentialsProviderChain : public AWSCredentialsProvider
        {
        public:
            virtual ~AWSCredentialsProviderChain() = default;

            /**
             * When a credentials provider in the chain returns empty credentials,
             * We go on to the next provider until we have either exhausted the installed providers in the chain or something returns non-empty credentials.
             */
            virtual AWSCredentials GetAWSCredentials();

        protected:
            /**
             * This class is only allowed to be initialized by subclasses.
             */
            AWSCredentialsProviderChain() = default;

            /**
             * Adds a provider to the back of the chain.
             */
            void AddProvider(const std::shared_ptr<AWSCredentialsProvider>& provider) { m_providerChain.push_back(provider); }

        private:            
            Aws::Vector<std::shared_ptr<AWSCredentialsProvider> > m_providerChain;
        };

        /**
         * Creates an AWSCredentialsProviderChain which uses in order EnvironmentAWSCredentialsProvider, ProfileConfigFileAWSCredentialsProvider,
         * and InstanceProfileCredentialsProvider.
         */
        class AWS_CORE_API DefaultAWSCredentialsProviderChain : public AWSCredentialsProviderChain
        {
        public:
            /**
             * Initializes the provider chain with EnvironmentAWSCredentialsProvider, ProfileConfigFileAWSCredentialsProvider,
             * and InstanceProfileCredentialsProvider in that order.
             */
            DefaultAWSCredentialsProviderChain();
        };

    } // namespace Auth
} // namespace Aws
