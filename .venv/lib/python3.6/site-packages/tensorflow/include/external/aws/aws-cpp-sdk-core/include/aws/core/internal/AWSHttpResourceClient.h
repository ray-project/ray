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
#include <aws/core/utils/memory/AWSMemory.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <memory>

namespace Aws
{
    namespace Http
    {
        class HttpClient;
    } // namespace Http

    namespace Internal
    {
        /**
         * Simple client for accessing the AWS remote data by Http.
         * Currently we use it to access EC2 Metadata and ECS Credential.
         */
        class AWS_CORE_API AWSHttpResourceClient
        {
        public:
            /**
             * Builds an AWSHttpResourceClient instance by using default http stack.
             */
            AWSHttpResourceClient(const char* logtag = "AWSHttpResourceClient");

            AWSHttpResourceClient& operator =(const AWSHttpResourceClient& rhs) = delete;
            AWSHttpResourceClient(const AWSHttpResourceClient& rhs) = delete;
            AWSHttpResourceClient& operator =(const AWSHttpResourceClient&& rhs) = delete;
            AWSHttpResourceClient(const AWSHttpResourceClient&& rhs) = delete;

            virtual ~AWSHttpResourceClient();

            /**
             * Connects to the metadata service to read the specified resource and
             * returns the text contents. The resource URI = endpoint + resourcePath. 
             * (e.g:http://domain/path/to/res)
             */
            virtual Aws::String GetResource(const char* endpoint, const char* resourcePath) const;

        protected:
            Aws::String m_logtag;

        private:         
            std::shared_ptr<Http::HttpClient> m_httpClient;
        };
        
        /**
         * Derived class to support retrieving of EC2 Metadata
         */
        class AWS_CORE_API EC2MetadataClient : public AWSHttpResourceClient
        {
        public:
            /**
             * Build an instance with default EC2 Metadata service endpoint
             */
            EC2MetadataClient(const char* endpoint = "http://169.254.169.254");

            EC2MetadataClient& operator =(const EC2MetadataClient& rhs) = delete;
            EC2MetadataClient(const EC2MetadataClient& rhs) = delete;
            EC2MetadataClient& operator =(const EC2MetadataClient&& rhs) = delete;
            EC2MetadataClient(const EC2MetadataClient&& rhs) = delete;

            virtual ~EC2MetadataClient();

            using AWSHttpResourceClient::GetResource;

            /**
            * Connects to the metadata service to read the specified resource and
            * returns the text contents. The resource URI = m_endpoint + resourcePath.
            */
            virtual Aws::String GetResource(const char* resourcePath) const;

            /**
             * Connects to the Amazon EC2 Instance Metadata Service to retrieve the
             * default credential information (if any).
             */
            virtual Aws::String GetDefaultCredentials() const;

            /**
             * connects to the Amazon EC2 Instance metadata Service to retrieve the region
             * the current EC2 instance is running in.
             */
            virtual Aws::String GetCurrentRegion() const;

        private:
            Aws::String m_endpoint;
        };

        /**
         * Derievd class to support retrieving of ECS Credentials
         */
        class AWS_CORE_API ECSCredentialsClient : public AWSHttpResourceClient
        {
        public:
            /**
             * Build an instance with default ECS service endpoint
             */
            ECSCredentialsClient(const char* resourcePath, const char* endpoint = "http://169.254.170.2");
            ECSCredentialsClient& operator =(ECSCredentialsClient& rhs) = delete;
            ECSCredentialsClient(const ECSCredentialsClient& rhs) = delete;
            ECSCredentialsClient& operator =(ECSCredentialsClient&& rhs) = delete;
            ECSCredentialsClient(const ECSCredentialsClient&& rhs) = delete;

            virtual ~ECSCredentialsClient();
            
            /**
             * Connects to the Amazon ECS service to retrieve the credential
             */
            virtual Aws::String GetECSCredentials() const 
            {
                return this->GetResource(m_endpoint.c_str(), m_resourcePath.c_str());
            }

        private:
            Aws::String m_resourcePath;
            Aws::String m_endpoint;
        };

    } // namespace Internal
} // namespace Aws
