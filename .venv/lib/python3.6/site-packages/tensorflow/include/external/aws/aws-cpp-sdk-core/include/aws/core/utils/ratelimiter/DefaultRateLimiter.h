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

#include <aws/core/utils/ratelimiter/RateLimiterInterface.h>
#include <aws/core/utils/memory/stl/AWSFunction.h>

#include <algorithm>
#include <mutex>
#include <thread>

namespace Aws
{
    namespace Utils
    {
        namespace RateLimits
        {
            /**
             * High precision rate limiter. If you need to limit your bandwidth by a budget, this is very likely the implementation you want.
             */
            template<typename CLOCK = std::chrono::high_resolution_clock, typename DUR = std::chrono::seconds, bool RENORMALIZE_RATE_CHANGES = true>
            class DefaultRateLimiter : public RateLimiterInterface
            {
            public:
                using Base = RateLimiterInterface;

                using InternalTimePointType = std::chrono::time_point<CLOCK>;
                using ElapsedTimeFunctionType = std::function< InternalTimePointType() >;

                /**
                 * Initializes state, starts counts, does some basic validation.
                 */
                DefaultRateLimiter(int64_t maxRate, ElapsedTimeFunctionType elapsedTimeFunction = AWS_BUILD_FUNCTION(CLOCK::now)) :
                    m_elapsedTimeFunction(elapsedTimeFunction),
                    m_maxRate(0),
                    m_accumulatorLock(),
                    m_accumulator(0),
                    m_accumulatorFraction(0),
                    m_accumulatorUpdated(),
                    m_replenishNumerator(0),
                    m_replenishDenominator(0),
                    m_delayNumerator(0),
                    m_delayDenominator(0)
                {
                    // verify we're not going to divide by zero due to goofy type parameterization
                    static_assert(DUR::period::num > 0, "Rate duration must have positive numerator");
                    static_assert(DUR::period::den > 0, "Rate duration must have positive denominator");
                    static_assert(CLOCK::duration::period::num > 0, "RateLimiter clock duration must have positive numerator");
                    static_assert(CLOCK::duration::period::den > 0, "RateLimiter clock duration must have positive denominator");

                    SetRate(maxRate, true);
                }

                virtual ~DefaultRateLimiter() = default;

                /**
                 * Calculates time in milliseconds that should be delayed before letting anymore data through.
                 */
                virtual DelayType ApplyCost(int64_t cost) override
                {
                    std::lock_guard<std::recursive_mutex> lock(m_accumulatorLock);

                    auto now = m_elapsedTimeFunction();
                    auto elapsedTime = (now - m_accumulatorUpdated).count();

                    // replenish the accumulator based on how much time has passed
                    auto temp = elapsedTime * m_replenishNumerator + m_accumulatorFraction;
                    m_accumulator += temp / m_replenishDenominator;
                    m_accumulatorFraction = temp % m_replenishDenominator;

                    // the accumulator is capped based on the maximum rate
                    m_accumulator = (std::min)(m_accumulator, m_maxRate);
                    if (m_accumulator == m_maxRate)
                    {
                        m_accumulatorFraction = 0;
                    }

                    // if the accumulator is still negative, then we'll have to wait
                    DelayType delay(0);
                    if (m_accumulator < 0)
                    {
                        delay = DelayType(-m_accumulator * m_delayDenominator / m_delayNumerator);
                    }

                    // apply the cost to the accumulator after the delay has been calculated; the next call will end up paying for our cost
                    m_accumulator -= cost;
                    m_accumulatorUpdated = now;

                    return delay;
                }

                /**
                 * Same as ApplyCost() but then goes ahead and sleeps the current thread.
                 */
                virtual void ApplyAndPayForCost(int64_t cost) override
                {
                    auto costInMilliseconds = ApplyCost(cost);
                    if(costInMilliseconds.count() > 0)
                    {
                        std::this_thread::sleep_for(costInMilliseconds);
                    }
                }

                /**
                 * Update the bandwidth rate to allow.
                 */
                virtual void SetRate(int64_t rate, bool resetAccumulator = false) override
                {
                    std::lock_guard<std::recursive_mutex> lock(m_accumulatorLock);

                    // rate must always be positive
                    rate = (std::max)(static_cast<int64_t>(1), rate);

                    if (resetAccumulator)
                    {
                        m_accumulator = rate;
                        m_accumulatorFraction = 0;
                        m_accumulatorUpdated = m_elapsedTimeFunction();
                    }
                    else
                    {
                        // sync the accumulator to current time
                        ApplyCost(0); // this call is why we need a recursive mutex

                        if (ShouldRenormalizeAccumulatorOnRateChange())
                        {
                            // now renormalize the accumulator and its fractional part against the new rate
                            // the idea here is we want to preserve the desired wait based on the previous rate
                            //
                            // As an example:
                            //  Say we had a rate of 100/s and our accumulator was -500 (ie the next ApplyCost would incur a 5 second delay)
                            //  If we change the rate to 1000/s and want to preserve that delay, we need to scale the accumulator to -5000
                            m_accumulator = m_accumulator * rate / m_maxRate;
                            m_accumulatorFraction = m_accumulatorFraction * rate / m_maxRate;
                        }
                    }

                    m_maxRate = rate;

                    // Helper constants that represent the amount replenished per CLOCK time period; use the gcd to reduce them in order to try and minimize the chance of integer overflow
                    m_replenishNumerator = m_maxRate * DUR::period::den * CLOCK::duration::period::num;
                    m_replenishDenominator = DUR::period::num * CLOCK::duration::period::den;
                    auto gcd = ComputeGCD(m_replenishNumerator, m_replenishDenominator);
                    m_replenishNumerator /= gcd;
                    m_replenishDenominator /= gcd;

                    // Helper constants that represent the delay per unit of costAccumulator; use the gcd to reduce them in order to try and minimize the chance of integer overflow
                    m_delayNumerator = m_maxRate * DelayType::period::num * DUR::period::den;
                    m_delayDenominator = DelayType::period::den * DUR::period::num;
                    gcd = ComputeGCD(m_delayNumerator, m_delayDenominator);
                    m_delayNumerator /= gcd;
                    m_delayDenominator /= gcd;
                }

            private:

                int64_t ComputeGCD(int64_t num1, int64_t num2) const
                {
                    // Euclid's
                    while (num2 != 0)
                    {
                        int64_t rem = num1 % num2;
                        num1 = num2;
                        num2 = rem;
                    }

                    return num1;
                }

                bool ShouldRenormalizeAccumulatorOnRateChange() const { return RENORMALIZE_RATE_CHANGES; }

                /// Function that returns the current time
                ElapsedTimeFunctionType m_elapsedTimeFunction;

                /// The rate we want to limit to
                int64_t m_maxRate;

                /// We need to pretty much lock everything while either setting the rate or applying a cost
                std::recursive_mutex m_accumulatorLock;

                /// Tracks how much "rate" we currently have to give; if this drops below zero then we start having to wait in order to perform operations and maintain the rate
                /// Replenishes over time based on m_maxRate
                int64_t m_accumulator;

                /// Updates can occur at any time, leading to a fractional accumulation; represents the fraction (m_accumulatorFraction / m_replenishDenominator)
                int64_t m_accumulatorFraction;

                /// Last time point the accumulator was updated
                InternalTimePointType m_accumulatorUpdated;

                /// Some helper constants that represents fixed (per m_maxRate) ratios used in the delay and replenishment calculations
                int64_t m_replenishNumerator;
                int64_t m_replenishDenominator;
                int64_t m_delayNumerator;
                int64_t m_delayDenominator;
            };

        } // namespace RateLimits
    } // namespace Utils
} // namespace Aws
