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

#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/core/utils/memory/stl/AWSString.h>

namespace Aws
{
    namespace External
    {
        namespace tinyxml2
        {
            class XMLNode;

            class XMLDocument;
        } // namespace tinyxml2
    } // namespace External
} // namespace Aws

namespace Aws
{
    template<typename PAYLOAD_TYPE>
    class AmazonWebServiceResult;
    namespace Client
    {
        enum class CoreErrors;
        template<typename ERROR_TYPE>
        class AWSError;
        class AWSXMLClient;
    } // namespace Client
    namespace Utils
    {
        namespace Xml
        {
            /**
             * Converts escaped xml text back to the original characters (e.g. < ! > = etc...)
             */
            AWS_CORE_API Aws::String DecodeEscapedXmlText(const Aws::String& textToDecode);

            class XmlDocument;

            /**
             * Node in an Xml Document
             */
            class AWS_CORE_API XmlNode
            {
            public:
                /**
                 * copies node and document over.
                 */
                XmlNode(const XmlNode& other);
                /**
                * copies node and document over.
                */
                XmlNode& operator=(const XmlNode& other);
                /**
                 * Get the name of the node.
                 */
                const Aws::String GetName() const;
                /**
                 * Set the name of the node.
                 */
                void SetName(const Aws::String& name);
                /**
                 * Get Value of an attribute specified by name.
                 */
                const Aws::String GetAttributeValue(const Aws::String& name) const;
                /**
                 * Set an attribute at name to value
                 */
                void SetAttributeValue(const Aws::String& name, const Aws::String& value);
                /**
                 * Get the inner text of the node (potentially includes other nodes)
                 */
                Aws::String GetText() const;
                /**
                 * Set the inner text of the node
                 */
                void SetText(const Aws::String& textValue);
                /**
                 * returns true if this node has another sibling.
                 */
                bool HasNextNode() const;
                /**
                 * returns the next sibling.
                 */
                XmlNode NextNode() const;
                /**
                 * returns the next sibling that matches node name.
                 */
                XmlNode NextNode(const char* name) const;
                /**
                 * returns the next sibling that matches node name.
                 */
                XmlNode NextNode(const Aws::String& name) const;
                /**
                 * return the first child node of this node.
                 */
                XmlNode FirstChild() const;
                /**
                 * returns the first child node of this node that has name.
                 */
                XmlNode FirstChild(const char* name) const;
                /**
                 * returns the first child node of this node that has name.
                 */
                XmlNode FirstChild(const Aws::String& name) const;
                /**
                 * returns true if this node has child nodes.
                 */
                bool HasChildren() const;
                /**
                 * returns the parent of this node.
                 */
                XmlNode Parent() const;
                /**
                 * Creates a new child element to this with name
                 */
                XmlNode CreateChildElement(const Aws::String& name);
                /**
                 * Creates a new child element to this with name
                 */
                XmlNode CreateSiblingElement(const Aws::String& name);
                /**
                 * If current node is valid.
                 */
                bool IsNull();

            private:
                XmlNode(Aws::External::tinyxml2::XMLNode* node, const XmlDocument& document) :
                    m_node(node), m_doc(&document)
                {
                }

                //we do not own these.... I just had to change it from ref because the compiler was
                //confused about which assignment operator to call. Do not... I repeat... do not delete
                //these pointers in your destructor.
                Aws::External::tinyxml2::XMLNode* m_node;
                const XmlDocument* m_doc;

                friend class XmlDocument;
            };

            /**
             * Container for Xml Document as a whole. All nodes have a reference to their parent document. Any changes
             * you make to the nodes will be reflected here. 
             */
            class AWS_CORE_API XmlDocument
            {
            public:
                /**
                 * move document memory
                 */
                XmlDocument(XmlDocument&& doc); 
                XmlDocument(const XmlDocument& other) = delete;

                ~XmlDocument();

                /**
                 * Get root element of the document
                 */
                XmlNode GetRootElement() const;
                /**
                 * Convert entire document to string. Use this if you for example, want to save the document to a file.
                 */
                Aws::String ConvertToString() const;
                /**
                 * Returns true if the call to CreateFromXml* was successful, otherwise false.
                 * if this returns false, you can call GetErrorMessage() to see details.
                 */
                bool WasParseSuccessful() const;
                /**
                 * Returns the error message if the call to CreateFromXml* failed. 
                 */
                Aws::String GetErrorMessage() const;
                /**
                 * Parses the stream into an XMLDocument
                 */
                static XmlDocument CreateFromXmlStream(Aws::IOStream&);
                /**
                * Parses the string into an XMLDocument
                */
                static XmlDocument CreateFromXmlString(const Aws::String&);
                /**
                * Creates an empty document with root node name
                */
                static XmlDocument CreateWithRootNode(const Aws::String&);

            private:
                XmlDocument();

                Aws::External::tinyxml2::XMLDocument* m_doc;

                friend class XmlNode;

                //allow outcome call the default constructor to handle it's error case.
                friend class Aws::Utils::Outcome<Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>, Aws::Client::AWSError<Aws::Client::CoreErrors>>;
                friend class Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>;
                friend class Client::AWSXMLClient;
            };

        } // namespace Xml
    } // namespace Utils
} // namespace Aws

