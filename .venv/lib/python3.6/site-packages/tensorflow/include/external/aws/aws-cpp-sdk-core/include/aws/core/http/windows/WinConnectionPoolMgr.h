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

#include <aws/core/utils/memory/stl/AWSMap.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/ResourceManager.h>

#include <utility>

namespace Aws
{
    namespace Http
    {

        /**
        * Connection pool manager for windows apis
        * maintains open connections for a given hostname and port.
        */
        class AWS_CORE_API WinConnectionPoolMgr
        {
        public:
            /*
            * Constructs instance, using iOpenHandle from InternetOpen api call, and maxConnectionsPerHost.
            */
            WinConnectionPoolMgr(void* iOpenHandle, unsigned maxConnectionsPerHost, long requestTimeout, long connectTimeout);

            /*
            * Cleans up all connections that have been allocated (might take a while).
            */
            virtual ~WinConnectionPoolMgr();

            /*
            * Aquires a connection for host and port from pool, or adds connections to pool until pool has reached max size
            * If no connections are available and the pool is at its maximum size, then this call will block until connections
            * become available.
            */
            void* AquireConnectionForHost(const Aws::String& host, uint16_t port);

            /*
            * Releases a connection to host and port back to the pool, if another thread is blocked in Aquire, then the top queued item
            * will be returned a connection and signaled to continue.
            */
            void ReleaseConnectionForHost(const Aws::String& host, unsigned port, void* connection);

            /**
             * Gets the tag for use for the logging system accross the various implementations of this class.
             */
            virtual const char* GetLogTag() const { return "ConnectionPoolMgr"; }
            /**
             * Gives an opportunity of implementations to make their api calls to cleanup a handle.
             */
            virtual void DoCloseHandle(void* handle) const = 0;
        protected:
            /**
             * Container for the connection pool. Allows tracking of number of connections per endpoint and also their handles.
             */
            class AWS_CORE_API HostConnectionContainer
            {
            public:
                uint16_t port;
                Aws::Utils::ExclusiveOwnershipResourceManager<void*> hostConnections;
                unsigned currentPoolSize;
            };

            /**
             * Gets the current global "open handle"
             */
            void* GetOpenHandle() const { return m_iOpenHandle; }

            /**
             * Gets the configured request timeout for this connection pool.
             */
            long GetRequestTimeout() const { return m_requestTimeoutMs; }
            /**
             * Gets the configured connection timeout for thsi connection pool.
             */
            long GetConnectTimeout() const { return m_connectTimeoutMs; }
            /**
             * Cleans up all open resources.
             */
            void DoCleanup();

        private:

            virtual void* CreateNewConnection(const Aws::String& host, HostConnectionContainer& connectionContainer) const = 0;

            WinConnectionPoolMgr(const WinConnectionPoolMgr&) = delete;
            const WinConnectionPoolMgr& operator = (const WinConnectionPoolMgr&) = delete;
            WinConnectionPoolMgr(const WinConnectionPoolMgr&&) = delete;
            const WinConnectionPoolMgr& operator = (const WinConnectionPoolMgr&&) = delete;

            bool CheckAndGrowPool(const Aws::String& host, HostConnectionContainer& connectionContainer);

            void* m_iOpenHandle;
            Aws::Map<Aws::String, HostConnectionContainer*> m_hostConnections;
            std::mutex m_hostConnectionsMutex;
            unsigned m_maxConnectionsPerHost;
            long m_requestTimeoutMs;
            long m_connectTimeoutMs;
            std::mutex m_containerLock;
        };

    } // namespace Http
} // namespace Aws
