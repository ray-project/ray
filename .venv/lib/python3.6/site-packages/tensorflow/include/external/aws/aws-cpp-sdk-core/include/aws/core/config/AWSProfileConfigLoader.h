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

#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/utils/DateTime.h>

namespace Aws
{
    namespace Internal
    {
        class EC2MetadataClient;
    }

    namespace Config
    {
        /**
         * Simple data container for a Profile.
         */
        class Profile
        {
        public:
            inline const Aws::String& GetName() const { return m_name; }
            inline void SetName(const Aws::String& value) { m_name = value; }
            inline const Aws::Auth::AWSCredentials& GetCredentials() const { return m_credentials; }
            inline void SetCredentials(const Aws::Auth::AWSCredentials& value) { m_credentials = value; }
            inline const Aws::String& GetRegion() const { return m_region; }
            inline void SetRegion(const Aws::String& value) { m_region = value; }
            inline const Aws::String& GetRoleArn() const { return m_roleArn; }
            inline void SetRoleArn(const Aws::String& value) { m_roleArn = value; }
            inline const Aws::String& GetSourceProfile() const { return m_sourceProfile; }
            inline void SetSourceProfile(const Aws::String& value ) { m_sourceProfile = value; }
            inline void SetAllKeyValPairs(const Aws::Map<Aws::String, Aws::String>& map) { m_allKeyValPairs = map; }
            inline const Aws::String GetValue(const Aws::String& key) 
            {
                auto iter = m_allKeyValPairs.find(key);
                if (iter == m_allKeyValPairs.end()) return "";
                return iter->second;
            }

        private:
            Aws::String m_name;
            Aws::String m_region;
            Aws::Auth::AWSCredentials m_credentials;
            Aws::String m_roleArn;
            Aws::String m_sourceProfile;

            Aws::Map<Aws::String, Aws::String> m_allKeyValPairs;
        };

        /**
         * Loads Configuration such as .aws/config, .aws/credentials or ec2 metadata service.
         */
        class AWS_CORE_API AWSProfileConfigLoader
        {
        public:
            virtual ~AWSProfileConfigLoader() = default;

            /**
             * Load the configuration
             */
            bool Load();

            /**
             * Over writes the entire config source with the newly configured profile data.
             */
            bool PersistProfiles(const Aws::Map<Aws::String, Aws::Config::Profile>& profiles);

            /**
             * Gets all profiles from the configuration file.
             */
            inline const Aws::Map<Aws::String, Aws::Config::Profile>& GetProfiles() const { return m_profiles; };

            /**
             * the timestamp from the last time the profile information was loaded from file.
             */
            inline const Aws::Utils::DateTime& LastLoadTime() const { return m_lastLoadTime; }

        protected:
            /**
             * Subclasses override this method to implement fetching the profiles.
             */
            virtual bool LoadInternal() = 0;

            /**
             * Subclasses override this method to implement persisting the profiles. Default returns false.
             */
            virtual bool PersistInternal(const Aws::Map<Aws::String, Aws::Config::Profile>&) { return false; }

            Aws::Map<Aws::String, Aws::Config::Profile> m_profiles;
            Aws::Utils::DateTime m_lastLoadTime;
        };

        /**
         * Reads configuration from a config file (e.g. $HOME/.aws/config or $HOME/.aws/credentials
         */
        class AWS_CORE_API AWSConfigFileProfileConfigLoader : public AWSProfileConfigLoader
        {
        public:
            /**
             * fileName - file to load config from
             * useProfilePrefix - whether or not the profiles are prefixed with "profile", credentials file is not
             * while the config file is. Defaults to off.
             */
            AWSConfigFileProfileConfigLoader(const Aws::String& fileName, bool useProfilePrefix = false);

            virtual ~AWSConfigFileProfileConfigLoader() = default;

            /**
             * File path being used for the config loader.
             */
            const Aws::String& GetFileName() const { return m_fileName; }

        protected:
            virtual bool LoadInternal() override;
            virtual bool PersistInternal(const Aws::Map<Aws::String, Aws::Config::Profile>&) override;

        private:
            Aws::String m_fileName;
            bool m_useProfilePrefix;
        };

        static const char* const INSTANCE_PROFILE_KEY = "InstanceProfile";

        /**
         * Loads configuration from the EC2 Metadata Service
         */
        class AWS_CORE_API EC2InstanceProfileConfigLoader : public AWSProfileConfigLoader
        {
        public:
            /**
             * If client is nullptr, the default EC2MetadataClient will be created.
             */
            EC2InstanceProfileConfigLoader(const std::shared_ptr<Aws::Internal::EC2MetadataClient>& = nullptr);

            virtual ~EC2InstanceProfileConfigLoader() = default;

        protected:
            virtual bool LoadInternal() override;

        private:
            std::shared_ptr<Aws::Internal::EC2MetadataClient> m_ec2metadataClient;
        };
    }
}
