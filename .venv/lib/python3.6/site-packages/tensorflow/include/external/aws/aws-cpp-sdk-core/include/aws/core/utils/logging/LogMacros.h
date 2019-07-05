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

#include <aws/core/utils/logging/LogLevel.h>
#include <aws/core/utils/logging/AWSLogging.h>
#include <aws/core/utils/logging/LogSystemInterface.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>

// While macros are usually grotty, using them here lets us have a simple function call interface for logging that
//
//  (1) Can be compiled out completely, so you don't even have to pay the cost to check the log level (which will be a virtual function call and a std::atomic<> read) if you don't want any AWS logging
//  (2) If you use logging and the log statement doesn't pass the conditional log filter level, not only do you not pay the cost of building the log string, you don't pay the cost for allocating or
//      getting any of the values used in building the log string, as they're in a scope (if-statement) that never gets entered.

#ifdef DISABLE_AWS_LOGGING

    #define AWS_LOG(level, tag, ...) 
    #define AWS_LOG_FATAL(tag, ...) 
    #define AWS_LOG_ERROR(tag, ...) 
    #define AWS_LOG_WARN(tag, ...) 
    #define AWS_LOG_INFO(tag, ...) 
    #define AWS_LOG_DEBUG(tag, ...) 
    #define AWS_LOG_TRACE(tag, ...) 

    #define AWS_LOGSTREAM(level, tag, streamExpression) 
    #define AWS_LOGSTREAM_FATAL(tag, streamExpression)
    #define AWS_LOGSTREAM_ERROR(tag, streamExpression)
    #define AWS_LOGSTREAM_WARN(tag, streamExpression)
    #define AWS_LOGSTREAM_INFO(tag, streamExpression)
    #define AWS_LOGSTREAM_DEBUG(tag, streamExpression)
    #define AWS_LOGSTREAM_TRACE(tag, streamExpression)

#else

    #define AWS_LOG(level, tag, ...) \
        { \
            Aws::Utils::Logging::LogSystemInterface* logSystem = Aws::Utils::Logging::GetLogSystem(); \
            if ( logSystem && logSystem->GetLogLevel() >= level ) \
            { \
                logSystem->Log(level, tag, __VA_ARGS__); \
            } \
        }

    #define AWS_LOG_FATAL(tag, ...) \
        { \
            Aws::Utils::Logging::LogSystemInterface* logSystem = Aws::Utils::Logging::GetLogSystem(); \
            if ( logSystem && logSystem->GetLogLevel() >= Aws::Utils::Logging::LogLevel::Fatal ) \
            { \
                logSystem->Log(Aws::Utils::Logging::LogLevel::Fatal, tag, __VA_ARGS__); \
            } \
        }

    #define AWS_LOG_ERROR(tag, ...) \
        { \
            Aws::Utils::Logging::LogSystemInterface* logSystem = Aws::Utils::Logging::GetLogSystem(); \
            if ( logSystem && logSystem->GetLogLevel() >= Aws::Utils::Logging::LogLevel::Error ) \
            { \
                logSystem->Log(Aws::Utils::Logging::LogLevel::Error, tag, __VA_ARGS__); \
            } \
        }

    #define AWS_LOG_WARN(tag, ...) \
        { \
            Aws::Utils::Logging::LogSystemInterface* logSystem = Aws::Utils::Logging::GetLogSystem(); \
            if ( logSystem && logSystem->GetLogLevel() >= Aws::Utils::Logging::LogLevel::Warn ) \
            { \
                logSystem->Log(Aws::Utils::Logging::LogLevel::Warn, tag, __VA_ARGS__); \
            } \
        }

    #define AWS_LOG_INFO(tag, ...) \
        { \
            Aws::Utils::Logging::LogSystemInterface* logSystem = Aws::Utils::Logging::GetLogSystem(); \
            if ( logSystem && logSystem->GetLogLevel() >= Aws::Utils::Logging::LogLevel::Info ) \
            { \
                logSystem->Log(Aws::Utils::Logging::LogLevel::Info, tag, __VA_ARGS__); \
            } \
        }

    #define AWS_LOG_DEBUG(tag, ...) \
        { \
            Aws::Utils::Logging::LogSystemInterface* logSystem = Aws::Utils::Logging::GetLogSystem(); \
            if ( logSystem && logSystem->GetLogLevel() >= Aws::Utils::Logging::LogLevel::Debug ) \
            { \
                logSystem->Log(Aws::Utils::Logging::LogLevel::Debug, tag, __VA_ARGS__); \
            } \
        }

    #define AWS_LOG_TRACE(tag, ...) \
        { \
            Aws::Utils::Logging::LogSystemInterface* logSystem = Aws::Utils::Logging::GetLogSystem(); \
            if ( logSystem && logSystem->GetLogLevel() >= Aws::Utils::Logging::LogLevel::Trace ) \
            { \
                logSystem->Log(Aws::Utils::Logging::LogLevel::Trace, tag, __VA_ARGS__); \
            } \
        }

    #define AWS_LOGSTREAM(level, tag, streamExpression) \
        { \
            Aws::Utils::Logging::LogSystemInterface* logSystem = Aws::Utils::Logging::GetLogSystem(); \
            if ( logSystem && logSystem->GetLogLevel() >= level ) \
            { \
                Aws::OStringStream logStream; \
                logStream << streamExpression; \
                logSystem->LogStream( logLevel, tag, logStream ); \
            } \
        }

    #define AWS_LOGSTREAM_FATAL(tag, streamExpression) \
        { \
            Aws::Utils::Logging::LogSystemInterface* logSystem = Aws::Utils::Logging::GetLogSystem(); \
            if ( logSystem && logSystem->GetLogLevel() >= Aws::Utils::Logging::LogLevel::Fatal ) \
            { \
                Aws::OStringStream logStream; \
                logStream << streamExpression; \
                logSystem->LogStream( Aws::Utils::Logging::LogLevel::Fatal, tag, logStream ); \
            } \
        }

    #define AWS_LOGSTREAM_ERROR(tag, streamExpression) \
        { \
            Aws::Utils::Logging::LogSystemInterface* logSystem = Aws::Utils::Logging::GetLogSystem(); \
            if ( logSystem && logSystem->GetLogLevel() >= Aws::Utils::Logging::LogLevel::Error ) \
            { \
                Aws::OStringStream logStream; \
                logStream << streamExpression; \
                logSystem->LogStream( Aws::Utils::Logging::LogLevel::Error, tag, logStream ); \
            } \
        }

    #define AWS_LOGSTREAM_WARN(tag, streamExpression) \
        { \
            Aws::Utils::Logging::LogSystemInterface* logSystem = Aws::Utils::Logging::GetLogSystem(); \
            if ( logSystem && logSystem->GetLogLevel() >= Aws::Utils::Logging::LogLevel::Warn ) \
            { \
                Aws::OStringStream logStream; \
                logStream << streamExpression; \
                logSystem->LogStream( Aws::Utils::Logging::LogLevel::Warn, tag, logStream ); \
            } \
        }

    #define AWS_LOGSTREAM_INFO(tag, streamExpression) \
        { \
            Aws::Utils::Logging::LogSystemInterface* logSystem = Aws::Utils::Logging::GetLogSystem(); \
            if ( logSystem && logSystem->GetLogLevel() >= Aws::Utils::Logging::LogLevel::Info ) \
            { \
                Aws::OStringStream logStream; \
                logStream << streamExpression; \
                logSystem->LogStream( Aws::Utils::Logging::LogLevel::Info, tag, logStream ); \
            } \
        }

    #define AWS_LOGSTREAM_DEBUG(tag, streamExpression) \
        { \
            Aws::Utils::Logging::LogSystemInterface* logSystem = Aws::Utils::Logging::GetLogSystem(); \
            if ( logSystem && logSystem->GetLogLevel() >= Aws::Utils::Logging::LogLevel::Debug ) \
            { \
                Aws::OStringStream logStream; \
                logStream << streamExpression; \
                logSystem->LogStream( Aws::Utils::Logging::LogLevel::Debug, tag, logStream ); \
            } \
        }

    #define AWS_LOGSTREAM_TRACE(tag, streamExpression) \
        { \
            Aws::Utils::Logging::LogSystemInterface* logSystem = Aws::Utils::Logging::GetLogSystem(); \
            if ( logSystem && logSystem->GetLogLevel() >= Aws::Utils::Logging::LogLevel::Trace ) \
            { \
                Aws::OStringStream logStream; \
                logStream << streamExpression; \
                logSystem->LogStream( Aws::Utils::Logging::LogLevel::Trace, tag, logStream ); \
            } \
        }

#endif // DISABLE_AWS_LOGGING
