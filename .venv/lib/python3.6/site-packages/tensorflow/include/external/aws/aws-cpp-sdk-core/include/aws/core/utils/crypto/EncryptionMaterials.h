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
#include <aws/core/utils/crypto/CryptoBuf.h>
#include <aws/core/utils/crypto/ContentCryptoMaterial.h>

namespace Aws
{
    namespace Utils
    {
        namespace Crypto
        {
            class AWS_CORE_API EncryptionMaterials
            {
            public:
                virtual ~EncryptionMaterials();

                /*
                * Override this method to control how to encrypt the content encryption key (CEK). This occurs in place.
                */
                virtual void EncryptCEK(ContentCryptoMaterial& contentCryptoMaterial) = 0;

                /*
                * Override this method to control how to decrypt the content encryption key (CEK). This occurs in place.
                */
                virtual void DecryptCEK(ContentCryptoMaterial& contentCryptoMaterial) = 0;
            };
        }//namespace Crypto
    }//namespace Utils
}//namespace Aws
