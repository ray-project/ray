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
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>

namespace Aws
{
    namespace Utils
    {
        namespace Stream
        {
            /**
             * Encapsulates and manages ownership of custom response streams. This is a move only type.
             */
            class AWS_CORE_API ResponseStream
            {
            public:
                /**
                 * sets underlying stream to nullptr
                 */
                ResponseStream();
                /**
                 * moves the underlying stream
                 */
                ResponseStream(ResponseStream&&);
                /**
                 * Uses factory to allocate underlying stream
                 */
                ResponseStream(const Aws::IOStreamFactory& factory);
                /**
                 * Takes ownership of an underlying stream.
                 */
                ResponseStream(IOStream* underlyingStreamToManage);
                ResponseStream(const ResponseStream&) = delete;
                ~ResponseStream();

                /**
                 * moves the underlying stream
                 */
                ResponseStream& operator=(ResponseStream&&);
                ResponseStream& operator=(const ResponseStream&) = delete;

                /**
                 * Gives access to underlying stream, but keep in mind that this changes state of the stream
                 */
                inline Aws::IOStream& GetUnderlyingStream() const { return *m_underlyingStream; }

            private:
                void ReleaseStream();

                Aws::IOStream* m_underlyingStream;
            };

            class AWS_CORE_API DefaultUnderlyingStream : public Aws::IOStream
            {
            public:
                using Base = Aws::IOStream;

                DefaultUnderlyingStream();
                virtual ~DefaultUnderlyingStream();
            };

            AWS_CORE_API Aws::IOStream* DefaultResponseStreamFactoryMethod();

        } //namespace Stream
    } //namespace Utils
} //namespace Aws
