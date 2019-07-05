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

#include <aws/core/utils/logging/LogLevel.h>
#include <aws/core/utils/logging/LogSystemInterface.h>
#include <aws/core/utils/memory/MemorySystemInterface.h>
#include <aws/core/utils/crypto/Factories.h>
#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/Core_EXPORTS.h>

namespace Aws
{
    static const char* DEFAULT_LOG_PREFIX = "aws_sdk_";

    /**
     * SDK wide options for logging
     */
    struct LoggingOptions
    {
        LoggingOptions() : logLevel(Aws::Utils::Logging::LogLevel::Off), defaultLogPrefix(DEFAULT_LOG_PREFIX)
        { }

        /**
         * Defaults to Off, if this is set to something else, then logging will be turned on and logLevel will be passed to the logger
         */
        Aws::Utils::Logging::LogLevel logLevel;

        /**
         * Defaults to aws_sdk_. This will only be used if the default logger is used.
         */
        const char* defaultLogPrefix;

        /**
         * Defaults to empty, if logLevel has been set and this field is empty, then the default log interface will be used.
         * otherwise, we will call this closure to create a logger
         */
         std::function<std::shared_ptr<Aws::Utils::Logging::LogSystemInterface>()> logger_create_fn;
    };

    /**
     * SDK wide options for memory management
     */
    struct MemoryManagementOptions
    {
        MemoryManagementOptions() : memoryManager(nullptr)
        { }

        /**
         * Defaults to nullptr. If custom memory management is being used and this hasn't been set then the default memory
         * manager will be used. If this has been set and custom memory management has been turned on, then this will be installed
         * at startup time.
         */
        Aws::Utils::Memory::MemorySystemInterface* memoryManager;
    };

    /**
     * SDK wide options for http
     */
    struct HttpOptions
    {
        HttpOptions() : initAndCleanupCurl(true), installSigPipeHandler(false)
        { }

        /**
         * Defaults to empty, if this is set, then the result of your closure will be installed and used instead of the system defaults
         */
        std::function<std::shared_ptr<Aws::Http::HttpClientFactory>()> httpClientFactory_create_fn;
        /**
        * libCurl infects everything with its global state. If it is being used then we automatically initialize and clean it up.
        * If this is a problem for you, set this to false. If you manually initialize libcurl please add the option CURL_GLOBAL_ALL to your init call.
        */
        bool initAndCleanupCurl;
        /**
         * Installs a global SIGPIPE handler that logs the error and prevents it from terminating the current process.
         * This can be used on operating systems on which CURL is being used. In some situations CURL cannot avoid 
         * triggering a SIGPIPE.
         * For more information see: https://curl.haxx.se/libcurl/c/CURLOPT_NOSIGNAL.html
         * NOTE: CURLOPT_NOSIGNAL is already being set.
         */
        bool installSigPipeHandler;
    };

    /**
     * SDK wide options for crypto
     */
    struct CryptoOptions
    {
        CryptoOptions() : initAndCleanupOpenSSL(true)
        { }

        /**
         * If set, this closure will be used to create and install the factory.
         */
        std::function<std::shared_ptr<Aws::Utils::Crypto::HashFactory>()> md5Factory_create_fn;
        /**
         * If set, this closure will be used to create and install the factory.
         */
        std::function<std::shared_ptr<Aws::Utils::Crypto::HashFactory>()> sha256Factory_create_fn;
        /**
         * If set, this closure will be used to create and install the factory.
         */
        std::function<std::shared_ptr<Aws::Utils::Crypto::HMACFactory>()> sha256HMACFactory_create_fn;
        /**
         * If set, this closure will be used to create and install the factory.
         */
        std::function<std::shared_ptr<Aws::Utils::Crypto::SymmetricCipherFactory>()> aes_CBCFactory_create_fn;
        /**
         * If set, this closure will be used to create and install the factory.
         */
        std::function<std::shared_ptr<Aws::Utils::Crypto::SymmetricCipherFactory>()> aes_CTRFactory_create_fn;
        /**
         * If set, this closure will be used to create and install the factory.
         */
        std::function<std::shared_ptr<Aws::Utils::Crypto::SymmetricCipherFactory>()> aes_GCMFactory_create_fn;
        /**
        * If set, this closure will be used to create and install the factory.
        */
        std::function<std::shared_ptr<Aws::Utils::Crypto::SymmetricCipherFactory>()> aes_KeyWrapFactory_create_fn;
        /**
         * If set, this closure will be used to create and install the factory.
         */
        std::function<std::shared_ptr<Aws::Utils::Crypto::SecureRandomFactory>()> secureRandomFactory_create_fn;
        /**
         * OpenSSL infects everything with its global state. If it is being used then we automatically initialize and clean it up.
         * If this is a problem for you, set this to false. Be aware that if you don't use our init and cleanup and you are using 
         * crypto functionality, you are responsible for installing thread locking, and loading strings and error messages.
         */
        bool initAndCleanupOpenSSL;
    };

    /**
     * You may notice that instead of taking pointers directly to your factories, we take a closure. This is because
     * if you have installed custom memory management, the allocation for your factories needs to happen after
     * the memory system has been initialized and shutdown needs to happen prior to the memory management being shutdown.
     *
     * Common Recipes:
     *
     * Just use defaults:
     *
     * SDKOptions options;
     * Aws::InitAPI(options);
     * .....
     * Aws::ShutdownAPI(options);
     *
     * Turn logging on using the default logger:
     *
     * SDKOptions options;
     * options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
     * Aws::InitAPI(options);
     * .....
     * Aws::ShutdownAPI(options);
     *
     * Install custom memory manager:
     *
     * MyMemoryManager memoryManager;
     *
     * SDKOptions options;
     * options.memoryManagementOptions.memoryManager = &memoryManager;
     * Aws::InitAPI(options);
     * .....
     * Aws::ShutdownAPI(options);
     *
     * Override default http client factory
     *
     * SDKOptions options;
     * options.httpOptions.httpClientFactory_create_fn = [](){ return Aws::MakeShared<MyCustomHttpClientFactory>("ALLOC_TAG", arg1); };
     * Aws::InitAPI(options);
     * .....
     * Aws::ShutdownAPI(options);
     */
    struct SDKOptions
    {
        /**
         * SDK wide options for logging
         */
        LoggingOptions loggingOptions;
        /**
         * SDK wide options for memory management
         */
        MemoryManagementOptions memoryManagementOptions;
        /**
         * SDK wide options for http
         */
        HttpOptions httpOptions;
        /**
         * SDK wide options for crypto
         */
        CryptoOptions cryptoOptions;
    };

    /*
     * Initialize SDK wide state for the SDK. This method must be called before doing anything else with this library.
     *
     * Common Recipes:
     *
     * Just use defaults:
     *
     * SDKOptions options;
     * Aws::InitAPI(options);
     * .....
     * Aws::ShutdownAPI(options);
     *
     * Turn logging on using the default logger:
     *
     * SDKOptions options;
     * options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
     * Aws::InitAPI(options);
     * .....
     * Aws::ShutdownAPI(options);
     *
     * Install custom memory manager:
     *
     * MyMemoryManager memoryManager;
     *
     * SDKOptions options;
     * options.memoryManagementOptions.memoryManager = &memoryManager;
     * Aws::InitAPI(options);
     * .....
     * Aws::ShutdownAPI(options);
     *
     * Override default http client factory
     *
     * SDKOptions options;
     * options.httpOptions.httpClientFactory_create_fn = [](){ return Aws::MakeShared<MyCustomHttpClientFactory>("ALLOC_TAG", arg1); };
     * Aws::InitAPI(options);
     * .....
     * Aws::ShutdownAPI(options);
     */
    AWS_CORE_API void InitAPI(const SDKOptions& options);

    /**
     * Shutdown SDK wide state for the SDK. This method must be called when you are finished using the SDK.
     * Do not call any other SDK methods after calling ShutdownAPI.
     */
    AWS_CORE_API void ShutdownAPI(const SDKOptions& options);
}

