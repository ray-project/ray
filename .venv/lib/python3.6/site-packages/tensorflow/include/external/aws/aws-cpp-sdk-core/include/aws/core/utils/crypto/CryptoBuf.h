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

#include <aws/core/utils/crypto/Cipher.h>
#include <aws/core/Core_EXPORTS.h>
#include <ios>

namespace Aws
{
    namespace Utils
    {
        namespace Crypto
        {
            typedef std::mbstate_t FPOS_TYPE;
            static const size_t DEFAULT_BUF_SIZE = 1024;
            static const size_t PUT_BACK_SIZE = 1;

            /**
             * Which mode a cipher is being used for. Encryption or Decryption
             */
            enum class CipherMode
            {
                Encrypt,
                Decrypt
            };

            /**
             * Generic base class for a streambuf that handles cryptography.
             */
            class AWS_CORE_API CryptoBuf : public std::streambuf
            {
            public:
                CryptoBuf() = default;
                virtual ~CryptoBuf() = default;
                CryptoBuf(const CryptoBuf&) = delete;
                CryptoBuf(CryptoBuf&& rhs) = delete;
                /**
                 * If the buffer needs to do a final push to the sink or needs to empty the cipher blocks etc...
                 */
                virtual void Finalize() {}
            };

            /**
             * StreamBuf that takes data from an input stream, encrypts or decrypts it, and causes all input received from the stream that is using it
             * to go through the cipher. A common use case is a file that is encrypted on disk being read via ifstream and then read decrypted into memory.
             * Or you could send a plain text file through an iostream and have it encrypted when the stream is read.
             *
             * This streambuf is seekable, but it is very expensive to seek backwards since we have to reset the cipher and re-encrypt everything up to that point.
             * So seeking should be avoided if at all possible.
             */
            class AWS_CORE_API SymmetricCryptoBufSrc : public CryptoBuf
            {
            public:
                /**
                 * stream to src from
                 * cipher to encrypt or decrypt the src stream with
                 * mode to use cipher in. Encryption or Decyption
                 * buffersize, the size of the src buffers to read at a time. Defaults to 1kb
                 */
                SymmetricCryptoBufSrc(Aws::IStream& stream, SymmetricCipher& cipher, CipherMode cipherMode, size_t bufferSize = DEFAULT_BUF_SIZE);

                SymmetricCryptoBufSrc(const SymmetricCryptoBufSrc&) = delete;
                SymmetricCryptoBufSrc(SymmetricCryptoBufSrc&&) = delete;

                SymmetricCryptoBufSrc& operator=(const SymmetricCryptoBufSrc&) = delete;
                SymmetricCryptoBufSrc& operator=(SymmetricCryptoBufSrc&&) = delete;

                virtual ~SymmetricCryptoBufSrc() { FinalizeCipher(); }

                /**
                 * This call isn't necessary if you loop over the stream.read until you reach EOF, if you happen to read the exact
                 * amount as the in memory buffers and finish the stream, but never finish the finalize call you'll need to be sure
                 * to call this. This is automatically called by the destructor.
                 */
                void Finalize() override { FinalizeCipher(); }

            protected:
                pos_type seekoff(off_type off, std::ios_base::seekdir dir, std::ios_base::openmode which = std::ios_base::in | std::ios_base::out ) override;
                pos_type seekpos(pos_type pos, std::ios_base::openmode which = std::ios_base::in | std::ios_base::out ) override;

            private:
                int_type underflow() override;
                off_type ComputeAbsSeekPosition(off_type, std::ios_base::seekdir,  std::fpos<FPOS_TYPE>);
                void FinalizeCipher();

                CryptoBuffer m_isBuf;
                SymmetricCipher& m_cipher;
                Aws::IStream& m_stream;
                CipherMode m_cipherMode;
                bool m_isFinalized;
                size_t m_bufferSize;
                size_t m_putBack;
            };

            /**
             * Stream buf that takes it's input, encrypts or decrypts it using the cipher, and writes it to the sink stream. This is particularly useful
             * when receiving an encrypted file over the internet and writing it decrypted to disk. Another case may be that you have an ofstream and you want
             * to write text to it from your program but have it go encrypted to disk.
             *
             * This stream buf is not seekable.
             */
            class AWS_CORE_API SymmetricCryptoBufSink : public CryptoBuf
            {
            public:
                /**
                 * stream, sink to push the encrypted or decrypted data to.
                 * cipher, symmetric cipher to use to transform the input before sending it to the sink.
                 * cipherMode, encrypt or decrypt
                 * bufferSize, amount of data to encrypt/decrypt at a time.
                 */
                SymmetricCryptoBufSink(Aws::OStream& stream, SymmetricCipher& cipher, CipherMode cipherMode, size_t bufferSize = DEFAULT_BUF_SIZE, int16_t blockOffset = 0);
                SymmetricCryptoBufSink(const SymmetricCryptoBufSink&) = delete;
                SymmetricCryptoBufSink(SymmetricCryptoBufSink&&) = delete;

                SymmetricCryptoBufSink& operator=(const SymmetricCryptoBufSink&) = delete;
                /**
                 * Not move assignable since it contains reference members
                 */
                SymmetricCryptoBufSink& operator=(SymmetricCryptoBufSink&&) = delete;

                virtual ~SymmetricCryptoBufSink();

                /**
                 * Finalizes the cipher and pushes all remaining data to the sink. Once this has been called, this streambuf cannot be used any further.
                 */
                void FinalizeCiphersAndFlushSink();
                /**
                 * Simply calls FinalizeCiphersAndFlushSink()
                 */
                void Finalize() override { FinalizeCiphersAndFlushSink(); }

            private:
                int_type overflow(int_type ch) override;
                int sync() override;
                bool writeOutput(bool finalize);

                CryptoBuffer m_osBuf;
                SymmetricCipher& m_cipher;
                Aws::OStream& m_stream;
                CipherMode m_cipherMode;
                bool m_isFinalized;
                int16_t m_blockOffset;
            };
        }
    }
}
