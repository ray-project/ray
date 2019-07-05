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

#include <utility>

namespace Aws
{
    namespace Utils
    {

        /**
         * Template class representing the outcome of making a request.  It will contain
         * either a successful result or the failure error.  The caller must check
         * whether the outcome of the request was a success before attempting to access
         *  the result or the error.
         */
        template<typename R, typename E> // Result, Error
        class Outcome
        {
        public:

            Outcome() : success(false)
            {
            } // Default constructor
            Outcome(const R& r) : result(r), success(true)
            {
            } // Result copy constructor
            Outcome(const E& e) : error(e), success(false)
            {
            } // Error copy constructor
            Outcome(R&& r) : result(std::forward<R>(r)), success(true)
            {
            } // Result move constructor
            Outcome(E&& e) : error(std::forward<E>(e)), success(false)
            {
            } // Error move constructor
            Outcome(const Outcome& o) :
                result(o.result),
                error(o.error),
                success(o.success)
            {
            }

            Outcome& operator=(const Outcome& o)
            {
                if (this != &o)
                {
                    result = o.result;
                    error = o.error;
                    success = o.success;
                }

                return *this;
            }

            Outcome(Outcome&& o) : // Required to force Move Constructor
                result(std::move(o.result)),
                error(std::move(o.error)),
                success(o.success)
            {
            }

            Outcome& operator=(Outcome&& o)
            {
                if (this != &o)
                {
                    result = std::move(o.result);
                    error = std::move(o.error);
                    success = o.success;
                }

                return *this;
            }

            inline const R& GetResult() const
            {
                return result;
            }

            inline R& GetResult()
            {
                return result;
            }

            /**
             * casts the underlying result to an r-value so that caller can take ownership of underlying resources.
             * this is necessary when streams are involved.
             */
            inline R&& GetResultWithOwnership()
            {
                return std::move(result);
            }

            inline const E& GetError() const
            {
                return error;
            }

            inline bool IsSuccess() const
            {
                return this->success;
            }

        private:
            R result;
            E error;
            bool success;
        };

    } // namespace Utils
} // namespace Aws
