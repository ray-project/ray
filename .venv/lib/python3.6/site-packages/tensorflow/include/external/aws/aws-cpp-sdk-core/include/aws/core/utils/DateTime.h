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
#include <aws/core/utils/memory/stl/AWSString.h>
#include <chrono>

namespace Aws
{
    namespace Utils
    {
        enum class DateFormat
        {
            RFC822, //for http headers
            ISO_8601, //for query and xml payloads
            AutoDetect
        };

        enum class Month
        {
            January = 0,
            February,
            March,
            April,
            May,
            June,
            July,
            August,
            September,
            October,
            November,
            December
        };

        enum class DayOfWeek
        {
            Sunday = 0,
            Monday,
            Tuesday,
            Wednesday,
            Thursday,
            Friday,
            Saturday
        };

        /**
         * Wrapper for all the weird crap we need to do with timestamps. 
         */
        class AWS_CORE_API DateTime
        {
        public:
            /**
             *  Initializes time point to epoch
             */
            DateTime();

            /**
            *  Initializes time point to any other arbirtrary timepoint
            */
            DateTime(const std::chrono::system_clock::time_point& timepointToAssign);

            /**
             * Initializes time point to millis Since epoch   
             */
            DateTime(int64_t millisSinceEpoch);

            /**
             * Initializes time point to epoch time in seconds.millis
             */
            DateTime(double epoch_millis);

            /**
             * Initializes time point to value represented by timestamp and format.
             */
            DateTime(const Aws::String& timestamp, DateFormat format);

            /**
             * Initializes time point to value represented by timestamp and format.
             */
            DateTime(const char* timestamp, DateFormat format);

            bool operator == (const DateTime& other) const;
            bool operator < (const DateTime& other) const;
            bool operator > (const DateTime& other) const;
            bool operator != (const DateTime& other) const;
            bool operator <= (const DateTime& other) const;
            bool operator >= (const DateTime& other) const;

            DateTime operator+(const std::chrono::milliseconds& a) const;
            DateTime operator-(const std::chrono::milliseconds& a) const;
            
            /**
             * Assign from seconds.millis since epoch.
             */
            DateTime& operator=(double secondsSinceEpoch);

            /**
             * Assign from millis since epoch.
             */
            DateTime& operator=(int64_t millisSinceEpoch);

            /**
            * Assign from another time_point
            */
            DateTime& operator=(const std::chrono::system_clock::time_point& timepointToAssign);

            /**
             * Whether or not parsing the timestamp from string was successful.
             */
            inline bool WasParseSuccessful() { return m_valid; }

            /**
             * Convert dateTime to local time string using predefined format.
             */
            Aws::String ToLocalTimeString(DateFormat format) const;

            /**
            * Convert dateTime to local time string using arbitrary format.
            */
            Aws::String ToLocalTimeString(const char* formatStr) const;

            /**
            * Convert dateTime to GMT time string using predefined format.
            */
            Aws::String ToGmtString(DateFormat format) const;

            /**
            * Convert dateTime to GMT time string using arbitrary format.
            */
            Aws::String ToGmtString(const char* formatStr) const;

            /**
             * Get the representation of this datetime as seconds.milliseconds since epoch
             */
            double SecondsWithMSPrecision() const;

            /**
             * Milliseconds since epoch of this datetime.
             */
            int64_t Millis() const;

            /**
             *  In the likely case this class doesn't do everything you need to do, here's a copy of the time_point structure. Have fun.
             */
            std::chrono::system_clock::time_point UnderlyingTimestamp() const;

            /**
             * Get the Year portion of this dateTime. localTime if true, return local time, otherwise return UTC
             */
            int GetYear(bool localTime = false) const;

            /**
            * Get the Month portion of this dateTime. localTime if true, return local time, otherwise return UTC
            */
            Month GetMonth(bool localTime = false) const;

            /**
            * Get the Day of the Month portion of this dateTime. localTime if true, return local time, otherwise return UTC
            */
            int GetDay(bool localTime = false) const;

            /**
            * Get the Day of the Week portion of this dateTime. localTime if true, return local time, otherwise return UTC
            */
            DayOfWeek GetDayOfWeek(bool localTime = false) const;

            /**
            * Get the Hour portion of this dateTime. localTime if true, return local time, otherwise return UTC
            */
            int GetHour(bool localTime = false) const;

            /**
            * Get the Minute portion of this dateTime. localTime if true, return local time, otherwise return UTC
            */
            int GetMinute(bool localTime = false) const;

            /**
            * Get the Second portion of this dateTime. localTime if true, return local time, otherwise return UTC
            */
            int GetSecond(bool localTime = false) const;

            /**
            * Get whether or not this dateTime is in Daylight savings time. localTime if true, return local time, otherwise return UTC
            */
            bool IsDST(bool localTime = false) const;

            /**
             * Get an instance of DateTime representing this very instant.
             */
            static DateTime Now(); 
            
            /**
             * Get the millis since epoch representing this very instant.
             */
            static int64_t CurrentTimeMillis();

            /**
             * Calculates the current local timestamp, formats it and returns it as a string
             */
            static Aws::String CalculateLocalTimestampAsString(const char* formatStr);

            /**
             * Calculates the current gmt timestamp, formats it, and returns it as a string
             */
            static Aws::String CalculateGmtTimestampAsString(const char* formatStr);

            /**
             * Calculates the current hour of the day in localtime.
             */
            static int CalculateCurrentHour();

            /**
             * The amazon timestamp format is a double with seconds.milliseconds
             */
            static double ComputeCurrentTimestampInAmazonFormat();

            /**
             * Compute the difference between two timestamps.
             */
            static std::chrono::milliseconds Diff(const DateTime& a, const DateTime& b);

        private:
            std::chrono::system_clock::time_point m_time;
            bool m_valid;
                        
            void ConvertTimestampStringToTimePoint(const char* timestamp, DateFormat format);
            tm GetTimeStruct(bool localTime) const;
            tm ConvertTimestampToLocalTimeStruct() const;
            tm ConvertTimestampToGmtStruct() const;   
        };

    } // namespace Utils
} // namespace Aws
