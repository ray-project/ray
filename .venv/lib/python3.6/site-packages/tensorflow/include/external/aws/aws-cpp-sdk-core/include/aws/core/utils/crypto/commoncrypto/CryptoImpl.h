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

#include <aws/core/utils/crypto/Hash.h>
#include <aws/core/utils/crypto/HMAC.h>
#include <aws/core/utils/crypto/SecureRandom.h>
#include <aws/core/utils/crypto/Cipher.h>

struct _CCCryptor;

namespace Aws
{
    namespace Utils
    {
        namespace Crypto
        {
            /**
             * Apple implementation for RandomBytes. Reads from /dev/random
             * This class is intentionally not thread safe. If you need to use an instance of this class from
             * multiple threads, then you are responsible for memory fencing.
             */
            class SecureRandomBytes_CommonCrypto : public SecureRandomBytes
            {
             public:
                /**
                 * Opens file descriptor to /dev/random
                 */
                SecureRandomBytes_CommonCrypto();
                /**
                 * Closes file descriptor
                 */
                ~SecureRandomBytes_CommonCrypto();
                /**
                 * Reads buffersize bytes from /dev/random and writes them to buffer
                 */
                void GetBytes(unsigned char* buffer, size_t bufferSize) override;

            private:
                FILE* fp;
            };


            class MD5CommonCryptoImpl : public Hash
            {
            public:

                MD5CommonCryptoImpl() {}
                virtual ~MD5CommonCryptoImpl() {}

                virtual HashResult Calculate(const Aws::String& str) override;

                virtual HashResult Calculate(Aws::IStream& stream) override;

            };

            class Sha256CommonCryptoImpl : public Hash
            {
            public:

                Sha256CommonCryptoImpl() {}
                virtual ~Sha256CommonCryptoImpl() {}

                virtual HashResult Calculate(const Aws::String& str) override;

                virtual HashResult Calculate(Aws::IStream& stream) override;
            };

            class Sha256HMACCommonCryptoImpl : public HMAC
            {
            public:

                Sha256HMACCommonCryptoImpl() {}
                virtual ~Sha256HMACCommonCryptoImpl() {}

                virtual HashResult Calculate(const ByteBuffer& toSign, const ByteBuffer& secret) override;


            };

            /**
             * CommonCrypto implementation for SymmetricCipher
             */
            class CommonCryptoCipher : public SymmetricCipher
            {
            public:
                /**
                 * Creates new CommonCrypto based cipher for key, and autogenerates a secure IV of size ivSize
                 */
                CommonCryptoCipher(const CryptoBuffer& key, size_t ivSize, bool ctrMode = false);

                /**
                 * Creates new CommonCrypto based cipher for key, initializationVector, and optional tag. If this is an authenticated
                 * cipher being used for decryption.
                 */
                CommonCryptoCipher(CryptoBuffer&& key, CryptoBuffer&& initializationVector,
                              CryptoBuffer&& tag = CryptoBuffer(0));

                /**
                 * Creates new CommonCrypto based cipher for key, initializationVector, and optional tag. If this is an authenticated
                 * cipher being used for decryption.
                 */
                CommonCryptoCipher(const CryptoBuffer& key, const CryptoBuffer& initializationVector,
                              const CryptoBuffer& tag = CryptoBuffer(0));

                CommonCryptoCipher(const CommonCryptoCipher& other) = delete;

                CommonCryptoCipher& operator=(const CommonCryptoCipher& other) = delete;

                /**
                 * Normally we don't work around VS 2013 not auto-generating these, but they are kind of expensive,
                 * so let's go ahead and optimize by defining default move operations. Implementors of this class
                 * need to be sure to define the move operations and call the base class.
                 */
                CommonCryptoCipher(CommonCryptoCipher&& toMove);

                /**
                 * Normally we don't work around VS 2013 not auto-generating these, but they are kind of expensive,
                 * so let's go ahead and optimize by defining default move operations. Implementors of this class
                 * need to be sure to define the move operations and call the base class.
                 */
                CommonCryptoCipher& operator=(CommonCryptoCipher&& toMove) = default;


                virtual ~CommonCryptoCipher();

                /**
                 * Encrypt a buffer of data. Part of the contract for this interface is that intention that
                 * a user call this function multiple times for a large stream. As such, multiple calls to this function
                 * on the same instance should produce valid sequential output for an encrypted stream.
                 */
                CryptoBuffer EncryptBuffer(const CryptoBuffer& unEncryptedData) override;

                /**
                 * Finalize Encryption, returns anything remaining in the last block
                 */
                CryptoBuffer FinalizeEncryption() override;

                /**
                * Decrypt a buffer of data. Part of the contract for this interface is that intention that
                * a user call this function multiple times for a large stream. As such, mutliple calls to this function
                * on the same instance should produce valid sequential output from an encrypted stream.
                */
                CryptoBuffer DecryptBuffer(const CryptoBuffer& encryptedData) override;

                /**
                 * Finalize Decryption, returns anything remaining in the last block
                 */
                CryptoBuffer FinalizeDecryption() override;

                void Reset() override;

            protected:
                /**
                 * Algorithm/Mode level config for the EVP_CIPHER_CTX
                 */
                virtual void InitEncryptor_Internal() = 0;

                /**
                 * Algorithm/Mode level config for the EVP_CIPHER_CTX
                 */
                virtual void InitDecryptor_Internal() = 0;

                virtual size_t GetBlockSizeBytes() const = 0;

                virtual size_t GetKeyLengthBits() const = 0;

                void CheckInitEncryptor();

                void CheckInitDecryptor();

                _CCCryptor* m_cryptoHandle;

            private:
                void Init();

                bool m_encDecInitialized;
                bool m_encryptionMode;
                bool m_decryptionMode;
            };

            /**
             * CommonCrypto implementation for AES in CBC mode
             */
            class AES_CBC_Cipher_CommonCrypto : public CommonCryptoCipher
            {
            public:
                /**
                 * Create AES in CBC mode off of a 256 bit key. Auto Generates a 16 byte secure random IV
                 */
                AES_CBC_Cipher_CommonCrypto(const CryptoBuffer& key);

                /**
                 * Create AES in CBC mode off of a 256 bit key and 16 byte IV
                 */
                AES_CBC_Cipher_CommonCrypto(CryptoBuffer&& key, CryptoBuffer&& initializationVector);

                /**
                 * Create AES in CBC mode off of a 256 bit key and 16 byte IV
                 */
                AES_CBC_Cipher_CommonCrypto(const CryptoBuffer& key, const CryptoBuffer& initializationVector);

                AES_CBC_Cipher_CommonCrypto(const AES_CBC_Cipher_CommonCrypto& other) = delete;

                AES_CBC_Cipher_CommonCrypto& operator=(const AES_CBC_Cipher_CommonCrypto& other) = delete;

                AES_CBC_Cipher_CommonCrypto(AES_CBC_Cipher_CommonCrypto&& toMove) = default;

            protected:
                void InitEncryptor_Internal() override;

                void InitDecryptor_Internal() override;

                size_t GetBlockSizeBytes() const override;

                size_t GetKeyLengthBits() const override;

            private:
                static size_t BlockSizeBytes;
                static size_t KeyLengthBits;
            };

            /**
             * CommonCrypto implementation for AES in CTR mode
             */
            class AES_CTR_Cipher_CommonCrypto : public CommonCryptoCipher
            {
            public:
                /**
                 * Create AES in CTR mode off of a 256 bit key. Auto Generates a 16 byte IV in the format
                 * [nonce 4bytes ] [securely random iv 8 bytes] [ CTR init 4bytes ]
                 */
                AES_CTR_Cipher_CommonCrypto(const CryptoBuffer& key);

                /**
                 * Create AES in CTR mode off of a 256 bit key and 16 byte IV
                 */
                AES_CTR_Cipher_CommonCrypto(CryptoBuffer&& key, CryptoBuffer&& initializationVector);

                /**
                 * Create AES in CTR mode off of a 256 bit key and 16 byte IV
                 */
                AES_CTR_Cipher_CommonCrypto(const CryptoBuffer& key, const CryptoBuffer& initializationVector);

                AES_CTR_Cipher_CommonCrypto(const AES_CTR_Cipher_CommonCrypto& other) = delete;

                AES_CTR_Cipher_CommonCrypto& operator=(const AES_CTR_Cipher_CommonCrypto& other) = delete;

                AES_CTR_Cipher_CommonCrypto(AES_CTR_Cipher_CommonCrypto&& toMove) = default;

            protected:
                void InitEncryptor_Internal() override;

                void InitDecryptor_Internal() override;

                size_t GetBlockSizeBytes() const override;

                size_t GetKeyLengthBits() const override;

            private:
                static size_t BlockSizeBytes;
                static size_t KeyLengthBits;
            };

            /**
             * CommonCrypto implementation for AES in KeyWrap mode. The key for the c_tor is the Kek,
             * it either encrypts a CEK or decrypts it.
             */
            class AES_KeyWrap_Cipher_CommonCrypto : public CommonCryptoCipher
            {
            public:
                /**
                 * Create AES in KeyWrap mode off of a 256 bit key.
                 */
                AES_KeyWrap_Cipher_CommonCrypto(const CryptoBuffer& key);

                AES_KeyWrap_Cipher_CommonCrypto(const AES_KeyWrap_Cipher_CommonCrypto&) = delete;

                AES_KeyWrap_Cipher_CommonCrypto& operator=(const AES_KeyWrap_Cipher_CommonCrypto&) = delete;

                AES_KeyWrap_Cipher_CommonCrypto(AES_KeyWrap_Cipher_CommonCrypto&&) = default;

                CryptoBuffer EncryptBuffer(const CryptoBuffer& unEncryptedData) override;

                CryptoBuffer FinalizeEncryption() override;

                CryptoBuffer DecryptBuffer(const CryptoBuffer& encryptedData) override;

                CryptoBuffer FinalizeDecryption() override;

                void Reset() override;

            protected:
                void InitEncryptor_Internal() override {};

                void InitDecryptor_Internal() override {};

                inline size_t GetBlockSizeBytes() const override { return BlockSizeBytes; }

                inline size_t GetKeyLengthBits() const override { return KeyLengthBits; }

            private:
                static size_t BlockSizeBytes;
                static size_t KeyLengthBits;

                CryptoBuffer m_workingKeyBuffer;
            };

        } // namespace Crypto
    } // namespace Utils
} // namespace Aws
