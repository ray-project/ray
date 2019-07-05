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
#include <aws/core/utils/Array.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>

namespace Aws
{
    namespace Utils
    {
        namespace Crypto
        {
            static const size_t SYMMETRIC_KEY_LENGTH = 32;
            static const size_t MIN_IV_LENGTH = 12;

            AWS_CORE_API CryptoBuffer IncrementCTRCounter(const CryptoBuffer& counter, uint32_t numberOfBlocks);

            /**
             * Interface for symmetric encryption and decryption providers. An instance of this class is good for exactly one encryption or decryption run.
             * It should not be used to encrypt or decrypt multiple messages.
             */
            class AWS_CORE_API SymmetricCipher
            {
            public:
                /**
                 * Initialize with key and an auto-generated initializationVector. Copies key.
                 *  ivGenerationInCtrMode, if true, initializes the iv with a 4 byte counter at the end.
                 */
                SymmetricCipher(const CryptoBuffer& key, size_t ivSize, bool ivGenerationInCtrMode = false) :
                        m_key(key), m_initializationVector(ivSize > 0 ? GenerateIV(ivSize, ivGenerationInCtrMode) : 0), m_failure(false) { Validate(); }

                /**
                 * Initialize with key and initializationVector, set tag for decryption of authenticated modes (makes copies of the buffers)
                 */
                SymmetricCipher(const CryptoBuffer& key, const CryptoBuffer& initializationVector, const CryptoBuffer& tag = CryptoBuffer(0)) :
                        m_key(key), m_initializationVector(initializationVector), m_tag(tag), m_failure(false) { Validate(); }

                /**
                 * Initialize with key and initializationVector, set tag for decryption of authenticated modes  (move the buffers)
                 */
                SymmetricCipher(CryptoBuffer&& key, CryptoBuffer&& initializationVector, CryptoBuffer&& tag = CryptoBuffer(0)) :
                        m_key(std::move(key)), m_initializationVector(std::move(initializationVector)), m_tag(std::move(tag)), m_failure(false) { Validate(); }

                SymmetricCipher(const SymmetricCipher& other) = delete;
                SymmetricCipher& operator=(const SymmetricCipher& other) = delete;

                /**
                 * Normally we don't work around VS 2013 not auto-generating these, but they are kind of expensive,
                 * so let's go ahead and optimize by defining default move operations. Implementors of this class
                 * need to be sure to define the move operations and call the base class.
                 */
                SymmetricCipher(SymmetricCipher&& toMove) :
                        m_key(std::move(toMove.m_key)),
                        m_initializationVector(std::move(toMove.m_initializationVector)),
                        m_tag(std::move(toMove.m_tag)),
                        m_failure(toMove.m_failure)
                {
                    Validate();
                }

                /**
                 * Normally we don't work around VS 2013 not auto-generating these, but they are kind of expensive,
                 * so let's go ahead and optimize by defining default move operations. Implementors of this class
                 * need to be sure to define the move operations and call the base class.
                 */
                SymmetricCipher& operator=(SymmetricCipher&& toMove)
                {
                    m_key = std::move(toMove.m_key);
                    m_initializationVector = std::move(toMove.m_initializationVector);
                    m_tag = std::move(toMove.m_tag);
                    m_failure = toMove.m_failure;

                    Validate();

                    return *this;
                }

                virtual ~SymmetricCipher() = default;

                /**
                 * Whether or not the cipher is in a good state. If this ever returns false, throw away all buffers
                 * it has vended.
                 */
                virtual operator bool() const { return Good(); }

                /**
                 * Encrypt a buffer of data. Part of the contract for this interface is that intention that 
                 * a user call this function multiple times for a large stream. As such, multiple calls to this function
                 * on the same instance should produce valid sequential output for an encrypted stream.
                 */
                virtual CryptoBuffer EncryptBuffer( const CryptoBuffer& unEncryptedData) = 0;

                /**
                 * Finalize Encryption, returns anything remaining in the last block
                 */
                virtual CryptoBuffer FinalizeEncryption () = 0;

                /**
                * Decrypt a buffer of data. Part of the contract for this interface is that intention that
                * a user call this function multiple times for a large stream. As such, multiple calls to this function
                * on the same instance should produce valid sequential output from an encrypted stream.
                */
                virtual CryptoBuffer DecryptBuffer(const CryptoBuffer& encryptedData) = 0;

                /**
                 * Finalize Decryption, returns anything remaining in the last block
                 */
                virtual CryptoBuffer FinalizeDecryption () = 0;

                virtual void Reset() = 0;

                /**
                 * IV used for encryption/decryption
                 */
                inline const CryptoBuffer& GetIV() const { return m_initializationVector; }

                /**
                 * Tag generated by encryption and used for the decryption.
                 *  This will be set in an authenticated mode, otherwise empty
                 */
                inline const CryptoBuffer& GetTag() const { return m_tag; }

                inline bool Fail() const { return m_failure; }
                inline bool Good() const { return !Fail(); }

                /**
                 * Generates a non-deterministic random IV. The first call is somewhat expensive but subsequent calls
                 * should be fast. If ctrMode is true, it will pad nonce in the first 1/4 of the iv and initialize
                 * the back 1/4 to 1.
                 */
                static CryptoBuffer GenerateIV(size_t ivLengthBytes, bool ctrMode = false);

                /**
                 * Generates a non-deterministic random symmetric key. Default (and minimum bar for security) is 256 bits.
                 */
                static CryptoBuffer GenerateKey(size_t keyLengthBytes = SYMMETRIC_KEY_LENGTH);                

            protected:
                SymmetricCipher() : m_failure(false) {}

                CryptoBuffer m_key;
                CryptoBuffer m_initializationVector;
                CryptoBuffer m_tag;
                bool m_failure;

            private:
                void Validate();
            };

            /**
             * Abstract factory class for Creating platform specific implementations of a Symmetric Cipher
             */
            class SymmetricCipherFactory
            {
            public:
                virtual ~SymmetricCipherFactory() {}

                /**
                 * Factory method. Returns cipher implementation. See the SymmetricCipher class for more details.
                 */
                virtual std::shared_ptr<SymmetricCipher> CreateImplementation(const CryptoBuffer& key) const = 0;
                /**
                 * Factory method. Returns cipher implementation. See the SymmetricCipher class for more details.
                 */
                virtual std::shared_ptr<SymmetricCipher> CreateImplementation(const CryptoBuffer& key, const CryptoBuffer& iv, const CryptoBuffer& tag = CryptoBuffer(0)) const = 0;
                /**
                 * Factory method. Returns cipher implementation. See the SymmetricCipher class for more details.
                 */
                virtual std::shared_ptr<SymmetricCipher> CreateImplementation(CryptoBuffer&& key, CryptoBuffer&& iv, CryptoBuffer&& tag = CryptoBuffer(0)) const = 0;

                /**
                 * Only called once per factory, your chance to make static library calls for setup.
                 * Default is no-op.
                 */
                virtual void InitStaticState() {}

                /**
                 * Only called once per factory, your chance to cleanup static library calls for setup.
                 * Default is no-op.
                 */
                virtual void CleanupStaticState() {}
            };
        }
    }
}