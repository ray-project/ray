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

#include <aws/core/utils/crypto/CryptoBuf.h>
#include <aws/core/Core_EXPORTS.h>

namespace Aws
{
    namespace Utils
    {
        namespace Crypto
        {
            /**
             * IOStream that can either take a src buffer as an input stream and create a wrapper iostream that can be used to receive encrypted or decrypted
             * input.
             *
             * A common use case is a file that is encrypted on disk being read via ifstream and then read decrypted into memory.
             * Or you could send a plain text file through an iostream and have it encrypted when the stream is read.
             *
             * This stream is seekable, but it is very expensive to seek backwards since we have to reset the cipher and rencrypt everything up to that point.
             * So seeking should be avoided if at all possible.
             *
             * Or it can be used in the opposite direction where the stream is a sink and all input to the stream will be encrypted or decrypted and then sent to the
             * sink stream.
             *
             * This is particularly useful
             * when receiving an encrypted file over the internet and writing it decrypted to disk. Another case may be that you have an ofstream and you want
             * to write text to it from your program but have it go encrypted to disk.
             *
             * In output mode, this stream is not seekable.
             */
            class AWS_CORE_API SymmetricCryptoStream : public Aws::IOStream
            {
            public:
                /**
                 * src stream to read from and encrypt or decrypt in transit
                 * mode of operation for cipher encryption or decryption
                 * cipher to use on the data
                 */
                SymmetricCryptoStream(Aws::IStream& src, CipherMode mode, SymmetricCipher& cipher, size_t bufLen = DEFAULT_BUF_SIZE);
                /**
                 * sink stream to write the data to and encrypt or decrypt in transit
                 * mode of operation for cipher encryption or decryption
                 * cipher to use on the data
                 */
                SymmetricCryptoStream(Aws::OStream& sink, CipherMode mode, SymmetricCipher& cipher, size_t bufLen = DEFAULT_BUF_SIZE, int16_t blockOffset = 0 );
                /**
                 * bufSrc streambuf to use
                 */
                SymmetricCryptoStream(Aws::Utils::Crypto::SymmetricCryptoBufSrc& bufSrc);
                /**
                 * bufSink streambuf to use
                 */
                SymmetricCryptoStream(Aws::Utils::Crypto::SymmetricCryptoBufSink& bufSink);

                SymmetricCryptoStream(const SymmetricCryptoStream&) = delete;
                SymmetricCryptoStream(SymmetricCryptoStream&&) = delete;

                virtual ~SymmetricCryptoStream();

                SymmetricCryptoStream& operator=(const SymmetricCryptoStream&) = delete;
                SymmetricCryptoStream& operator=(SymmetricCryptoStream&&) = delete;


                /**
                 * Call this in output stream mode when you want to flush the output to file.
                 * It will be called by the destructor, but if you want to sync before then, call this method.
                 * This stream is unusable after this method has been called.
                 */
                void Finalize();

            private:
                CryptoBuf* m_cryptoBuf;
                bool m_hasOwnership;
            };
        }
    }
}