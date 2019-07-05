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

#include <aws/core/utils/stream/SimpleStreamBuf.h>

#include <iostream>

namespace Aws
{

class AWS_CORE_API SimpleStringStream : public std::iostream
{
    public:

        using char_type	= char;
        using traits_type = std::char_traits<char>;
        using allocator_type = Aws::Allocator<char>;
        using int_type = int;
        using pos_type = std::streampos;
        using off_type = std::streamoff;

        using base = std::iostream;

        SimpleStringStream();
        SimpleStringStream(const Aws::String& value);

        virtual ~SimpleStringStream() {}

        // disabling copies of course
        SimpleStringStream(const SimpleStringStream& rhs) = delete;
        SimpleStringStream& operator=(const SimpleStringStream& rhs) = delete;

        // also disabling moves until there's a valid need
        SimpleStringStream(SimpleStringStream&& rhs) = delete;
        SimpleStringStream& operator=(SimpleStringStream&& rhs) = delete;

        Aws::Utils::Stream::SimpleStreamBuf* rdbuf() const { return const_cast<Aws::Utils::Stream::SimpleStreamBuf*>(&m_streamBuffer); }

        Aws::String str() { return m_streamBuffer.str(); }
        void str(const Aws::String value);

    private:

        Aws::Utils::Stream::SimpleStreamBuf m_streamBuffer;
};

class AWS_CORE_API SimpleIStringStream : public std::istream
{
    public:

        using char_type	= char;
        using traits_type = std::char_traits<char>;
        using allocator_type = Aws::Allocator<char>;
        using int_type = int;
        using pos_type = std::streampos;
        using off_type = std::streamoff;

        using base = std::istream;

        SimpleIStringStream();
        SimpleIStringStream(const Aws::String& value);

        virtual ~SimpleIStringStream() {}

        // disabling copies of course
        SimpleIStringStream(const SimpleIStringStream& rhs) = delete;
        SimpleIStringStream& operator=(const SimpleIStringStream& rhs) = delete;

        // also disabling moves until there's a valid need
        SimpleIStringStream(SimpleIStringStream&& rhs) = delete;
        SimpleIStringStream& operator=(SimpleIStringStream&& rhs) = delete;

        Aws::Utils::Stream::SimpleStreamBuf* rdbuf() const { return const_cast<Aws::Utils::Stream::SimpleStreamBuf*>(&m_streamBuffer); }

        Aws::String str() { return m_streamBuffer.str(); }
        void str(const Aws::String value);

    private:

        Aws::Utils::Stream::SimpleStreamBuf m_streamBuffer;
};

class AWS_CORE_API SimpleOStringStream : public std::ostream
{
    public:

        using char_type	= char;
        using traits_type = std::char_traits<char>;
        using allocator_type = Aws::Allocator<char>;
        using int_type = int;
        using pos_type = std::streampos;
        using off_type = std::streamoff;

        using base = std::ostream;

        SimpleOStringStream();
        SimpleOStringStream(const Aws::String& value);

        virtual ~SimpleOStringStream() {}

        // disabling copies of course
        SimpleOStringStream(const SimpleOStringStream& rhs) = delete;
        SimpleOStringStream& operator=(const SimpleOStringStream& rhs) = delete;

        // also disabling moves until there's a valid need
        SimpleOStringStream(SimpleOStringStream&& rhs) = delete;
        SimpleOStringStream& operator=(SimpleOStringStream&& rhs) = delete;

        Aws::Utils::Stream::SimpleStreamBuf* rdbuf() const { return const_cast<Aws::Utils::Stream::SimpleStreamBuf*>(&m_streamBuffer); }

        Aws::String str() { return m_streamBuffer.str(); }
        void str(const Aws::String value);

    private:

        Aws::Utils::Stream::SimpleStreamBuf m_streamBuffer;
};

} // namespace Aws

