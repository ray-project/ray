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
#include <aws/core/utils/UnreferencedParam.h>
#include <aws/core/utils/memory/MemorySystemInterface.h>

#include <memory>
#include <cstdlib>
#include <algorithm>
#include <type_traits>

namespace Aws
{
    namespace Utils
    {
        namespace Memory
        {
            /**
             *InitializeAWSMemory should be called at the very start of your program
             */
            AWS_CORE_API void InitializeAWSMemorySystem(MemorySystemInterface& memorySystem);

            /**
             * ShutdownAWSMemory should be called the very end of your program
             */
            AWS_CORE_API void ShutdownAWSMemorySystem(void);

            /**
             * Get the globally install memory system, if it has been installed.
             */
            AWS_CORE_API MemorySystemInterface* GetMemorySystem();

        } // namespace Memory
    } // namespace Utils

    /**
     * ::new, ::delete, ::malloc, ::free, std::make_shared, and std::make_unique should not be used in SDK code
     *  use these functions instead or Aws::MakeShared
     */
    AWS_CORE_API void* Malloc(const char* allocationTag, size_t allocationSize);

    /**
     * ::new, ::delete, ::malloc, ::free, std::make_shared, and std::make_unique should not be used in SDK code
     *  use these functions instead or Aws::MakeShared
     */
    AWS_CORE_API void Free(void* memoryPtr);

    /**
     * ::new, ::delete, ::malloc, ::free, std::make_shared, and std::make_unique should not be used in SDK code
     *  use these functions instead or Aws::MakeShared
     */
    template<typename T, typename ...ArgTypes>
    T* New(const char* allocationTag, ArgTypes&&... args)
    {
        void *rawMemory = Malloc(allocationTag, sizeof(T));
        // http://stackoverflow.com/questions/6783993/placement-new-and-delete
        T *constructedMemory = new (rawMemory) T(std::forward<ArgTypes>(args)...);
        return constructedMemory;
    }
/*
 * dynamic_cast of all sorts do not work on MSVC if RTTI is turned off.
 * This means that deleting an object via a pointer to one of its polymorphic base types that do not point to the 
 * address of their concrete class will result in undefined behavior.
 * Example:
 * struct Foo : InterfaceA, InterfaceB {};
 * Aws::Delete(pointerToInterfaceB);
 */
#if defined(_MSC_VER) && !defined(_CPPRTTI)
    template<typename T>
    void Delete(T* pointerToT)
    {
        if (pointerToT == nullptr)
        {
            return;
        }
        pointerToT->~T();
        Free(pointerToT);
    }
#else
    /**
     * ::new, ::delete, ::malloc, ::free, std::make_shared, and std::make_unique should not be used in SDK code
     *  use these functions instead or Aws::MakeShared
     */
    template<typename T>
    typename std::enable_if<!std::is_polymorphic<T>::value>::type Delete(T* pointerToT)
    {
        if (pointerToT == nullptr)
        {
            return;
        }
        //http://stackoverflow.com/questions/6783993/placement-new-and-delete
        pointerToT->~T();
        Free(pointerToT);
    }

    template<typename T>
    typename std::enable_if<std::is_polymorphic<T>::value>::type Delete(T* pointerToT)
    {
        if (pointerToT == nullptr)
        {
            return;
        }
        // deal with deleting objects that implement multiple interfaces
        // see casting to pointer to void in http://en.cppreference.com/w/cpp/language/dynamic_cast
        // https://stackoverflow.com/questions/8123776/are-there-practical-uses-for-dynamic-casting-to-void-pointer
        // NOTE: on some compilers, calling the destructor before doing the dynamic_cast affects how calculation of
        // the address of the most derived class.
        void* mostDerivedT = dynamic_cast<void*>(pointerToT);
        pointerToT->~T();
        Free(mostDerivedT);
    }
#endif // _CPPRTTI

    template<typename T>
    bool ShouldConstructArrayMembers()
    {
        return std::is_class<T>::value;
    }

    template<typename T>
    bool ShouldDestroyArrayMembers()
    {
        return !std::is_trivially_destructible<T>::value;
    }

    /**
     * ::new, ::delete, ::malloc, ::free, std::make_shared, and std::make_unique should not be used in SDK code
     *  use these functions instead or Aws::MakeShared
     */
    template<typename T>
    T* NewArray(std::size_t amount, const char* allocationTag)
    {
        if (amount > 0)
        {
            bool constructMembers = ShouldConstructArrayMembers<T>();
            bool trackMemberCount = ShouldDestroyArrayMembers<T>();

            // if we need to remember the # of items in the array (because we need to call their destructors) then allocate extra memory and keep the # of items in the extra slot
            std::size_t allocationSize = amount * sizeof(T);
#if defined(_MSC_VER) && _MSC_VER < 1900
            std::size_t headerSize = (std::max)(sizeof(std::size_t), __alignof(T));
#else
            std::size_t headerSize = (std::max)(sizeof(std::size_t), alignof(T));
#endif

            if (trackMemberCount)
            {
                allocationSize += headerSize;
            }

            void* rawMemory = Malloc(allocationTag, allocationSize);
            T* pointerToT = nullptr;

            if (trackMemberCount)
            {
                std::size_t* pointerToAmount = reinterpret_cast<std::size_t*>(rawMemory);
                *pointerToAmount = amount;
                pointerToT = reinterpret_cast<T*>(reinterpret_cast<char*>(pointerToAmount) + headerSize);

            }
            else
            {
                pointerToT = reinterpret_cast<T*>(rawMemory);
            }

            if (constructMembers)
            {
                for (std::size_t i = 0; i < amount; ++i)
                {
                    new (pointerToT + i) T;
                }
            }

            return pointerToT;
        }

        return nullptr;
    }

    /**
     * ::new, ::delete, ::malloc, ::free, std::make_shared, and std::make_unique should not be used in SDK code
     *  use these functions instead or Aws::MakeShared
     */
    template<typename T>
    void DeleteArray(T* pointerToTArray)
    {
        if (pointerToTArray == nullptr)
        {
            return;
        }

        bool destroyMembers = ShouldDestroyArrayMembers<T>();
        void* rawMemory = nullptr;

        if (destroyMembers)
        {
#if defined(_MSC_VER) && _MSC_VER < 1900
            std::size_t headerSize = (std::max)(sizeof(std::size_t), __alignof(T));
#else
            std::size_t headerSize = (std::max)(sizeof(std::size_t), alignof(T));
#endif

            std::size_t *pointerToAmount = reinterpret_cast<std::size_t*>(reinterpret_cast<char*>(pointerToTArray) - headerSize);
            std::size_t amount = *pointerToAmount;

            for (std::size_t i = amount; i > 0; --i)
            {
                (pointerToTArray + i - 1)->~T();
            }
            rawMemory = reinterpret_cast<void *>(pointerToAmount);
        }
        else
        {
            rawMemory = reinterpret_cast<void *>(pointerToTArray);
        }

        Free(rawMemory);
    }

    /**
     * modeled from std::default_delete
     */
    template<typename T>
    struct Deleter
    {
        Deleter() {}

        template<class U, class = typename std::enable_if<std::is_convertible<U *, T *>::value, void>::type>
        Deleter(const Deleter<U>&)
        {
        }

        void operator()(T *pointerToT) const
        {
            static_assert(0 < sizeof(T), "can't delete an incomplete type");
            Aws::Delete(pointerToT);
        }
    };

    template< typename T > using UniquePtr = std::unique_ptr< T, Deleter< T > >;

    /**
     * ::new, ::delete, ::malloc, ::free, std::make_shared, and std::make_unique should not be used in SDK code
     *  use these functions instead or Aws::MakeShared
     */
    template<typename T, typename ...ArgTypes>
    UniquePtr<T> MakeUnique(const char* allocationTag, ArgTypes&&... args)
    {
        return UniquePtr<T>(Aws::New<T>(allocationTag, std::forward<ArgTypes>(args)...));
    }

    template<typename T>
    struct ArrayDeleter
    {
        ArrayDeleter() {}

        template<class U, class = typename std::enable_if<std::is_convertible<U *, T *>::value, void>::type>
        ArrayDeleter(const ArrayDeleter<U>&)
        {
        }

        void operator()(T *pointerToTArray) const
        {
            static_assert(0 < sizeof(T), "can't delete an incomplete type");
            Aws::DeleteArray(pointerToTArray);
        }
    };

    template< typename T > using UniqueArrayPtr = std::unique_ptr< T, ArrayDeleter< T > >;

    /**
     * ::new, ::delete, ::malloc, ::free, std::make_shared, and std::make_unique should not be used in SDK code
     *  use these functions instead or Aws::MakeShared
     */
    template<typename T, typename ...ArgTypes>
    UniqueArrayPtr<T> MakeUniqueArray(std::size_t amount, const char* allocationTag, ArgTypes&&... args)
    {
        return UniqueArrayPtr<T>(Aws::NewArray<T>(amount, allocationTag, std::forward<ArgTypes>(args)...));
    }


} // namespace Aws

