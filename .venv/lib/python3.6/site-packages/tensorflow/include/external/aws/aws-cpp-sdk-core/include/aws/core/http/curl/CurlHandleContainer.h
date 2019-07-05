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

#include <aws/core/utils/ResourceManager.h>

#include <utility>
#include <curl/curl.h>

namespace Aws
{
namespace Http
{

/**
  * Simple Connection pool manager for Curl. It maintains connections in a thread safe manner. You
  * can call into acquire a handle, then put it back when finished. It is assumed that reusing an already
  * initialized handle is preferable (especially for synchronous clients). The pool doubles in capacity as
  * needed up to the maximum amount of connections.
  */
class CurlHandleContainer
{
public:
    /**
      * Initializes an empty stack of CURL handles. If you are only making synchronous calls via your http client
      * then a small size is best. For async support, a good value would be 6 * number of Processors.   *
      */
    CurlHandleContainer(unsigned maxSize = 50, long requestTimeout = 3000, long connectTimeout = 1000);
    ~CurlHandleContainer();

    /**
      * Blocks until a curl handle from the pool is available for use.
      */
    CURL* AcquireCurlHandle();
    /**
      * Returns a handle to the pool for reuse. It is imperative that this is called
      * after you are finished with the handle.
      */
    void ReleaseCurlHandle(CURL* handle);

private:
    CurlHandleContainer(const CurlHandleContainer&) = delete;
    const CurlHandleContainer& operator = (const CurlHandleContainer&) = delete;
    CurlHandleContainer(const CurlHandleContainer&&) = delete;
    const CurlHandleContainer& operator = (const CurlHandleContainer&&) = delete;

    bool CheckAndGrowPool();
    void SetDefaultOptionsOnHandle(CURL* handle);

    Aws::Utils::ExclusiveOwnershipResourceManager<CURL*> m_handleContainer;
    unsigned m_maxPoolSize;
    unsigned long m_requestTimeout;
    unsigned long m_connectTimeout;
    unsigned m_poolSize;
    std::mutex m_containerLock;
};

} // namespace Http
} // namespace Aws

