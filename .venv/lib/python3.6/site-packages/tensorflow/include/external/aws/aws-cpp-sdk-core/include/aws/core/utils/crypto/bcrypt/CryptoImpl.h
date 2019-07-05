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
#include <aws/core/utils/crypto/Hash.h>
#include <aws/core/utils/crypto/HMAC.h>
#include <aws/core/utils/crypto/Cipher.h>
#include <aws/core/utils/crypto/SecureRandom.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <mutex>

#if (defined(WIN32) || defined(_WIN32) || defined(__WIN32__))

#define WIN32_NO_STATUS 
#include <windows.h> 
#include <bcrypt.h>
#include <winternl.h>
#include <winerror.h>
#undef WIN32_NO_STATUS
#endif // AWS_SDK_PLATFORM_WINDOWS

namespace Aws
{
    namespace Utils
    {
        namespace Crypto
        {
            static const char* SecureRandom_BCrypt_Tag = "SecureRandom_BCrypt";

            class SecureRandomBytes_BCrypt : public SecureRandomBytes
            {
            public:
                SecureRandomBytes_BCrypt();
                ~SecureRandomBytes_BCrypt();
                void GetBytes(unsigned char* buffer, size_t bufferSize) override;
            private:
                BCRYPT_ALG_HANDLE m_algHandle;
            };

            class BCryptHashContext;

            /** RAII class for persistent data (can be reused across hash calculations) used in Windows cryptographic hash implementations
             *  If a mutex-free implementation is desired then this data won't be reusable like this
             */
            class BCryptHashImpl
            {
            public:
                /**
                 * Inititializes Windows Crypto APIs and gets the instance ready to perform crypto calculations.
                 * algorithmName is one of the values described here: https://msdn.microsoft.com/en-us/library/windows/desktop/aa375534(v=vs.85).aspx
                 */
                BCryptHashImpl(LPCWSTR algorithmName, bool isHMAC);
                ~BCryptHashImpl();

                /**
                 * Calculates a Hash on str.
                 */
                HashResult Calculate(const Aws::String& str);
                /**
                 * Calculates an HMAC on toHash using secret
                 */
                HashResult Calculate(const ByteBuffer& toHash, const ByteBuffer& secret);
                /**
                 * Calculates a Hash on the stream without loading the entire stream into memory at once.
                 */
                HashResult Calculate(Aws::IStream& stream);

            private:

                bool IsValid() const;

                HashResult HashData(const BCryptHashContext& context, PBYTE data, ULONG dataLength);
                bool HashStream(Aws::IStream& stream);

                void* m_algorithmHandle;

                DWORD m_hashBufferLength;
                PBYTE m_hashBuffer;

                DWORD m_hashObjectLength;
                PBYTE m_hashObject;

                //I'm 99% sure the algorithm handle for windows is not thread safe, but I can't 
                //prove or disprove that theory. Therefore, we have to lock to be safe.
                std::mutex m_algorithmMutex;
            };

            /**
             * Bcrypt based Md5 hash implementation.
             */
            class MD5BcryptImpl : public Hash
            {
            public:
                /**
                 * Initializes the algorithm handles and sets up state. You don't want to call this very often
                 */
                MD5BcryptImpl();
                virtual ~MD5BcryptImpl() {}

                /**
                 * Calculates md5 hash on str.
                 */
                virtual HashResult Calculate(const Aws::String& str) override;
                /**
                 * Calculates a md5 hash on the stream without loading the entire stream into memory at once.
                 */
                virtual HashResult Calculate(Aws::IStream& stream) override;

            private:
                BCryptHashImpl m_impl;
            };

            /**
            * Bcrypt based sha256 hash implementation.
            */
            class Sha256BcryptImpl : public Hash
            {
            public:
                /**
                 * Initializes the algorithm handles and sets up state. You don't want to call this very often
                 */
                Sha256BcryptImpl();
                virtual ~Sha256BcryptImpl() {}

                /**
                 * Calculates sha256 hash on str.
                 */
                virtual HashResult Calculate(const Aws::String& str) override;
                /**
                 * Calculates a sha256 hash on the stream without loading the entire stream into memory at once.
                 */
                virtual HashResult Calculate(Aws::IStream& stream) override;

            private:
                BCryptHashImpl m_impl;
            };

            /**
            * Bcrypt based sha256 hmac hash implementation.
            */
            class Sha256HMACBcryptImpl : public HMAC
            {
            public:

                /**
                 * Initializes the algorithm handles and sets up state. You don't want to call this very often
                 */
                Sha256HMACBcryptImpl();
                virtual ~Sha256HMACBcryptImpl() {}

                /**
                 * Calculates an sha256 HMAC on toSign using secret
                 */
                virtual HashResult Calculate(const ByteBuffer& toSign, const ByteBuffer& secret) override;

            private:
                BCryptHashImpl m_impl;
            };

            /**
             * Encryptor/Decrypto for AES.
             */
            class BCryptSymmetricCipher : public SymmetricCipher
            {
            public:
                BCryptSymmetricCipher(const CryptoBuffer& key, size_t ivSize, bool ctrMode = false);

                /**
                * Initialize with key and initializationVector, set tag for decryption of authenticated modes (makes copies of the buffers)
                */
                BCryptSymmetricCipher(const CryptoBuffer& key, const CryptoBuffer& initializationVector, const CryptoBuffer& tag = CryptoBuffer(0));

                /**
                * Initialize with key and initializationVector, set tag for decryption of authenticated modes  (move the buffers)
                */
                BCryptSymmetricCipher(CryptoBuffer&& key, CryptoBuffer&& initializationVector, CryptoBuffer&& tag = std::move(CryptoBuffer(0)));

                BCryptSymmetricCipher(const BCryptSymmetricCipher&) = delete;
                BCryptSymmetricCipher& operator=(const BCryptSymmetricCipher&) = delete;

                /**
                * Normally we don't work around VS 2013 not auto-generating these, but they are kind of expensive,
                * so let's go ahead and optimize by defining default move operations. Implementors of this class
                * need to be sure to define the move operations and call the base class.
                */
                BCryptSymmetricCipher(BCryptSymmetricCipher&& toMove);

                /**
                * Normally we don't work around VS 2013 not auto-generating these, but they are kind of expensive,
                * so let's go ahead and optimize by defining default move operations. Implementors of this class
                * need to be sure to define the move operations and call the base class.
                */
                BCryptSymmetricCipher& operator=(BCryptSymmetricCipher&& toMove);

                virtual ~BCryptSymmetricCipher();

                /**
                 * You should call this multiple times until you run out of data. Call FinalizeEncryption() when finished to recieve any remaining data.
                 * Once you call this method the first time, it can not ever be used with DecryptBuffer()
                 */
                CryptoBuffer EncryptBuffer(const CryptoBuffer& unEncryptedData) override;
                /**
                 * Some ciphers have remaining blocks that need to be cleared, call this after calling EncryptBuffer one or more times. The potential data returned here
                 * is part of your encrypted message. Once you call this method, you can not use this instance any more.
                 */
                CryptoBuffer FinalizeEncryption() override;
                /**
                 * You should call this multiple times until you run out of data. Call FinalizeDecryption() when finished to recieve any remaining data.
                 * Once you call this method the first time, it can not ever be used with EncryptBuffer()
                 */
                CryptoBuffer DecryptBuffer(const CryptoBuffer& encryptedData) override;
                /**
                 * Some ciphers have remaining blocks that need to be cleared, call this after calling DecryptBuffer one or more times. The potential data returned here
                 * is part of your decrypted message. Once you call this method, you can not use this instance any more.
                 */
                CryptoBuffer FinalizeDecryption() override;

                void Reset() override;

            protected:
                /**
                * Algorithm/Mode level config for the BCRYPT_ALG_HANDLE and BCRYPT_KEY_HANDLE
                */
                virtual void InitEncryptor_Internal() = 0;
                virtual void InitDecryptor_Internal() = 0;
                virtual size_t GetBlockSizeBytes() const = 0;
                virtual size_t GetKeyLengthBits() const = 0;

                void CheckInitEncryptor();
                void CheckInitDecryptor();

                BCRYPT_ALG_HANDLE m_algHandle;
                BCRYPT_KEY_HANDLE m_keyHandle;
                DWORD m_flags;
                CryptoBuffer m_workingIv;
                PBCRYPT_AUTHENTICATED_CIPHER_MODE_INFO m_authInfoPtr;
                bool m_encDecInitialized;
                bool m_encryptionMode;
                bool m_decryptionMode;

                static BCRYPT_KEY_HANDLE ImportKeyBlob(BCRYPT_ALG_HANDLE handle, CryptoBuffer& key);

            private:
                void Init();
                void InitKey();
                void Cleanup();
            };

            /**
             * BCrypt implementation for AES in CBC mode
             */
            class AES_CBC_Cipher_BCrypt : public BCryptSymmetricCipher
            {
            public:
                /**
                 * Create AES in CBC mode off of a 256 bit key. Auto Generates a 16 byte secure random IV
                 */
                AES_CBC_Cipher_BCrypt(const CryptoBuffer& key);

                /**
                * Create AES in CBC mode off of a 256 bit key and 16 byte IV
                */
                AES_CBC_Cipher_BCrypt(CryptoBuffer&& key, CryptoBuffer&& initializationVector);

                /**
                * Create AES in CBC mode off of a 256 bit key and 16 byte IV
                */
                AES_CBC_Cipher_BCrypt(const CryptoBuffer& key, const CryptoBuffer& initializationVector);

                AES_CBC_Cipher_BCrypt(const AES_CBC_Cipher_BCrypt&) = delete;

                AES_CBC_Cipher_BCrypt& operator=(const AES_CBC_Cipher_BCrypt&) = delete;

                AES_CBC_Cipher_BCrypt(AES_CBC_Cipher_BCrypt&& toMove) : BCryptSymmetricCipher(std::move(toMove)), m_blockOverflow(std::move(toMove.m_blockOverflow)) {}

                CryptoBuffer EncryptBuffer(const CryptoBuffer& unEncryptedData) override;
                CryptoBuffer FinalizeEncryption() override;
                CryptoBuffer DecryptBuffer(const CryptoBuffer& encryptedData) override;
                CryptoBuffer FinalizeDecryption() override;

                void Reset() override;

            protected:
                void InitEncryptor_Internal() override;
                void InitDecryptor_Internal() override;
                size_t GetBlockSizeBytes() const override;
                size_t GetKeyLengthBits() const override;

            private:
                CryptoBuffer FillInOverflow(const CryptoBuffer& buffer);

                CryptoBuffer m_blockOverflow;

                static size_t BlockSizeBytes;
                static size_t KeyLengthBits;
            };

            /**
             * BCrypt implementation for AES in CTR mode
             */
            class AES_CTR_Cipher_BCrypt : public BCryptSymmetricCipher
            {
            public:
                /**
                * Create AES in CTR mode off of a 256 bit key. Auto Generates a 16 byte IV in the format
                * [nonce 4bytes ] [securely random iv 8 bytes] [ CTR init 4bytes ]
                */
                AES_CTR_Cipher_BCrypt(const CryptoBuffer& key);

                /**
                * Create AES in CTR mode off of a 256 bit key and 16 byte IV
                */
                AES_CTR_Cipher_BCrypt(CryptoBuffer&& key, CryptoBuffer&& initializationVector);

                /**
                * Create AES in CTR mode off of a 256 bit key and 16 byte IV
                */
                AES_CTR_Cipher_BCrypt(const CryptoBuffer& key, const CryptoBuffer& initializationVector);

                AES_CTR_Cipher_BCrypt(const AES_CTR_Cipher_BCrypt&) = delete;

                AES_CTR_Cipher_BCrypt& operator=(const AES_CTR_Cipher_BCrypt&) = delete;

                AES_CTR_Cipher_BCrypt(AES_CTR_Cipher_BCrypt&& toMove) : BCryptSymmetricCipher(std::move(toMove)), m_blockOverflow(std::move(toMove.m_blockOverflow)) {}

                CryptoBuffer EncryptBuffer(const CryptoBuffer& unEncryptedData) override;
                CryptoBuffer FinalizeEncryption() override;
                CryptoBuffer DecryptBuffer(const CryptoBuffer& encryptedData) override;
                CryptoBuffer FinalizeDecryption() override;

                void Reset() override;

            protected:
                void InitEncryptor_Internal() override;
                void InitDecryptor_Internal() override;

                size_t GetBlockSizeBytes() const override;
                size_t GetKeyLengthBits() const override;

            private:                
                static void InitBuffersToNull(Aws::Vector<ByteBuffer*>& initBuffers);
                static void CleanupBuffers(Aws::Vector<ByteBuffer*>& cleanupBuffers);

                CryptoBuffer EncryptWithCtr(const CryptoBuffer& buffer);

                static size_t BlockSizeBytes;
                static size_t KeyLengthBits;

                CryptoBuffer m_blockOverflow;
            };

            /**
            * BCrypt implementation for AES in GCM mode
            */
            class AES_GCM_Cipher_BCrypt : public BCryptSymmetricCipher
            {
            public:
                /**
                * Create AES in GCM mode off of a 256 bit key. Auto Generates a 16 byte IV in the format
                */
                AES_GCM_Cipher_BCrypt(const CryptoBuffer& key);

                /**
                * Create AES in GCM mode off of a 256 bit key, 16 byte IV, and tag
                */
                AES_GCM_Cipher_BCrypt(CryptoBuffer&& key, CryptoBuffer&& initializationVector, CryptoBuffer&& tag = std::move(CryptoBuffer()));

                /**
                * Create AES in GCM mode off of a 256 bit key, 16 byte IV, and tag
                */
                AES_GCM_Cipher_BCrypt(const CryptoBuffer& key, const CryptoBuffer& initializationVector, const CryptoBuffer& tag = CryptoBuffer());

                AES_GCM_Cipher_BCrypt(const AES_GCM_Cipher_BCrypt&) = delete;

                AES_GCM_Cipher_BCrypt& operator=(const AES_GCM_Cipher_BCrypt&) = delete;

                AES_GCM_Cipher_BCrypt(AES_GCM_Cipher_BCrypt&& toMove) : 
                    BCryptSymmetricCipher(std::move(toMove)), m_macBuffer(std::move(toMove.m_macBuffer)), m_finalBuffer(std::move(toMove.m_finalBuffer)),
                    m_authInfo(std::move(toMove.m_authInfo)) {}

                CryptoBuffer EncryptBuffer(const CryptoBuffer&) override;
                CryptoBuffer FinalizeEncryption() override;
                CryptoBuffer DecryptBuffer(const CryptoBuffer&) override;
                CryptoBuffer FinalizeDecryption() override;

                void Reset() override;

            protected:
                void InitEncryptor_Internal() override;
                void InitDecryptor_Internal() override;

                size_t GetBlockSizeBytes() const override;
                size_t GetKeyLengthBits() const override;
                size_t GetTagLengthBytes() const;

            private:
                void InitCipher();

                static size_t BlockSizeBytes;
                static size_t NonceSizeBytes;
                static size_t KeyLengthBits;
                static size_t TagLengthBytes;

                CryptoBuffer m_macBuffer;
                CryptoBuffer m_finalBuffer;
                BCRYPT_AUTHENTICATED_CIPHER_MODE_INFO m_authInfo;
            };

            /**
             * BCrypt implementation for AES in KeyWrap mode. The key for the c_tor is the Kek,
             * it either encrypts a CEK or decrypts it.
            */
            class AES_KeyWrap_Cipher_BCrypt : public BCryptSymmetricCipher
            {
            public:
                /**
                 * Create AES in KeyWrap mode off of a 256 bit key.
                 * key - key encryption key               
                 */
                AES_KeyWrap_Cipher_BCrypt(const CryptoBuffer& key);

                AES_KeyWrap_Cipher_BCrypt(const AES_KeyWrap_Cipher_BCrypt&) = delete;

                AES_KeyWrap_Cipher_BCrypt& operator=(const AES_KeyWrap_Cipher_BCrypt&) = delete;

                AES_KeyWrap_Cipher_BCrypt(AES_CTR_Cipher_BCrypt&& toMove) : BCryptSymmetricCipher(std::move(toMove)) {}

                CryptoBuffer EncryptBuffer(const CryptoBuffer& unEncryptedData) override;
                CryptoBuffer FinalizeEncryption() override;
                CryptoBuffer DecryptBuffer(const CryptoBuffer& encryptedData) override;
                CryptoBuffer FinalizeDecryption() override;

                void Reset() override;

            protected:
                void InitEncryptor_Internal() override;
                void InitDecryptor_Internal() override;

                size_t GetBlockSizeBytes() const override;
                size_t GetKeyLengthBits() const override; 
                
            private:
                static size_t BlockSizeBytes; 
                static size_t KeyLengthBits;
                
                CryptoBuffer m_operatingKeyBuffer;
            };
        } // namespace Crypto
    } // namespace Utils
} // namespace Aws

