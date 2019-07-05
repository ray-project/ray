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

#include <aws/core/Aws.h>
#include <aws/core/Core_EXPORTS.h>
#include <aws/core/utils/crypto/CryptoBuf.h>
#include <aws/core/utils/crypto/ContentCryptoScheme.h>
#include <aws/core/utils/crypto/KeyWrapAlgorithm.h>

namespace Aws
{
    namespace Utils
    {
        namespace Crypto
        {
            class AWS_CORE_API ContentCryptoMaterial
            {
            public:
                /*
                Default constructor.
                */
                ContentCryptoMaterial();
                /*
                Initialize content crypto material with content crypto scheme. Constructor will also generate the cek automatically.
                Since the creation of the crypto content material will be within the S3 crypto modules, only the crypto scheme is needed for initialization.
                The rest of the data will be set using the accessors below.
                */
                ContentCryptoMaterial(ContentCryptoScheme contentCryptoScheme);

                /*
                Intialize with content encryption key (cek) and content crypto scheme.
                */
                ContentCryptoMaterial(const Aws::Utils::CryptoBuffer& cek, ContentCryptoScheme contentCryptoScheme);

                /**
                * Gets the underlying content encryption key.
                */
                inline const Aws::Utils::CryptoBuffer& GetContentEncryptionKey() const
                {
                    return m_contentEncryptionKey;
                }

                /**
                * Gets the underlying encrypted content encryption key.
                */
                inline const Aws::Utils::CryptoBuffer& GetEncryptedContentEncryptionKey() const
                {
                    return m_encryptedContentEncryptionKey;
                }

                /**
                * Gets the underlying initialization vector
                */
                inline const Aws::Utils::CryptoBuffer& GetIV() const
                {
                    return m_iv;
                }

                /**
                * Gets the underlying crypto tag length
                */
                inline size_t GetCryptoTagLength() const
                {
                    return m_cryptoTagLength;
                }

                /**
                * Gets the underlying materials description map.
                */
                inline const Aws::Map<Aws::String, Aws::String>& GetMaterialsDescription() const
                {
                    return m_materialsDescription;
                }

                /*
                * Gets the value of the key in the current materials description map.
                */
                inline const Aws::String& GetMaterialsDescription(const Aws::String& key) const
                {
                    return m_materialsDescription.at(key);
                }

                /**
                * Gets the underlying key wrap algorithm
                */
                inline KeyWrapAlgorithm GetKeyWrapAlgorithm() const
                {
                    return m_keyWrapAlgorithm;
                }

                /**
                * Gets the underlying content crypto scheme.
                */
                inline ContentCryptoScheme GetContentCryptoScheme() const
                {
                    return m_contentCryptoScheme;
                }

                /**
                * Sets the underlying content encryption key. Copies from parameter content encryption key.
                */
                inline void SetContentEncryptionKey(const Aws::Utils::CryptoBuffer& contentEncryptionKey)
                {
                    m_contentEncryptionKey = contentEncryptionKey;
                }

                /**
                * Sets the underlying encrypted content encryption key. Copies from parameter encrypted content encryption key.
                */
                inline void SetEncryptedContentEncryptionKey(const Aws::Utils::CryptoBuffer& encryptedContentEncryptionKey)
                {
                    m_encryptedContentEncryptionKey = encryptedContentEncryptionKey;
                }

                /**
                * Sets the underlying iv. Copies from parameter iv.
                */
                inline void SetIV(const Aws::Utils::CryptoBuffer& iv)
                {
                    m_iv = iv;
                }

                /**
                * Sets the underlying crypto Tag Length. Copies from parameter cryptoTagLength.
                */
                inline void SetCryptoTagLength(size_t cryptoTagLength)
                {
                    m_cryptoTagLength = cryptoTagLength;
                }

                /**
                * Adds to the current materials description map using a key and a value.
                */
                inline void AddMaterialsDescription(const Aws::String& key, const Aws::String& value)
                {
                    m_materialsDescription[key] = value;
                }

                /**
                * Sets the underlying materials description. Copies from parameter materials description.
                */
                inline void SetMaterialsDescription(const Aws::Map<Aws::String, Aws::String>& materialsDescription)
                {
                    m_materialsDescription = materialsDescription;
                }

                /**
                * Sets the underlying Key Wrap Algorithm. Copies from parameter keyWrapAlgorithm.
                */
                inline void SetKeyWrapAlgorithm(KeyWrapAlgorithm keyWrapAlgorithm)
                {
                    m_keyWrapAlgorithm = keyWrapAlgorithm;
                }

                /**
                * Sets the underlying content Crypto Scheme. Copies from parameter contentCryptoScheme.
                */
                inline void SetContentCryptoScheme(ContentCryptoScheme contentCryptoScheme)
                {
                    m_contentCryptoScheme = contentCryptoScheme;
                }

            private:
                Aws::Utils::CryptoBuffer m_contentEncryptionKey;
                Aws::Utils::CryptoBuffer m_encryptedContentEncryptionKey;
                Aws::Utils::CryptoBuffer m_iv;
                size_t m_cryptoTagLength;
                Aws::Map<Aws::String, Aws::String> m_materialsDescription;
                KeyWrapAlgorithm m_keyWrapAlgorithm;
                ContentCryptoScheme m_contentCryptoScheme;
            };
        }
    }
}