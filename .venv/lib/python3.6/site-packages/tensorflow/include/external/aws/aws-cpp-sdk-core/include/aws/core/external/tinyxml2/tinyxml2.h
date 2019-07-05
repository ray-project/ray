/*
Original code by Lee Thomason (www.grinninglizard.com)

This software is provided 'as-is', without any express or implied
warranty. In no event will the authors be held liable for any
damages arising from the use of this software.

Permission is granted to anyone to use this software for any
purpose, including commercial applications, and to alter it and
redistribute it freely, subject to the following restrictions:

1. The origin of this software must not be misrepresented; you must
not claim that you wrote the original software. If you use this
software in a product, an acknowledgment in the product documentation
would be appreciated but is not required.

2. Altered source versions must be plainly marked as such, and
must not be misrepresented as being the original software.

3. This notice may not be removed or altered from any source
distribution.
*/
/*
This file has been modified from its original version by Amazon:
  (1) Memory management operations use aws memory management api
  (2) Import-export preproccesor logic tweaked for better integration into core library
  (3) Wrapped everything in Amazon namespace to prevent static linking issues if the user includes a version of this code through another dependency
*/
#ifndef TINYXML2_INCLUDED
#define TINYXML2_INCLUDED

#if defined(ANDROID_NDK) || defined(__BORLANDC__) || defined(__QNXNTO__)
#   include <ctype.h>
#   include <limits.h>
#   include <stdio.h>
#   include <stdlib.h>
#   include <string.h>
#   include <stdarg.h>
#else
#   include <cctype>
#   include <climits>
#   include <cstdio>
#   include <cstdlib>
#   include <cstring>
#   include <cstdarg>
#endif

#include <aws/core/utils/memory/AWSMemory.h>

/*
TODO: intern strings instead of allocation.
*/
/*
gcc:
g++ -Wall -DDEBUG tinyxml2.cpp xmltest.cpp -o gccxmltest.exe

Formatting, Artistic Style:
AStyle.exe --style=1tbs --indent-switches --break-closing-brackets --indent-preprocessor tinyxml2.cpp tinyxml2.h
*/

#if defined( _DEBUG ) || defined( DEBUG ) || defined (__DEBUG__)
#   ifndef DEBUG
#       define DEBUG
#   endif
#endif

#ifdef _MSC_VER
#   pragma warning(push)
#   pragma warning(disable: 4251)
#endif

#ifdef _WIN32
    #ifdef USE_IMPORT_EXPORT
        #ifdef AWS_CORE_EXPORTS
            #define TINYXML2_LIB __declspec(dllexport)
        #else
            #define TINYXML2_LIB __declspec(dllimport)
        #endif // AWS_CORE_EXPORTS
    #endif // USE_IMPORT_EXPORT
#endif

#ifndef TINYXML2_LIB
    #define TINYXML2_LIB
#endif // TINYXML2_LIB


#if defined(DEBUG)
#   if defined(_MSC_VER)
#       // "(void)0," is for suppressing C4127 warning in "assert(false)", "assert(true)" and the like
#       define TIXMLASSERT( x )           if ( !((void)0,(x))) { __debugbreak(); } //if ( !(x)) WinDebugBreak()
#   elif defined (ANDROID_NDK)
#       include <android/log.h>
#       define TIXMLASSERT( x )           if ( !(x)) { __android_log_assert( "assert", "grinliz", "ASSERT in '%s' at %d.", __FILE__, __LINE__ ); }
#   else
#       include <assert.h>
#       define TIXMLASSERT                assert
#   endif
#   else
#       define TIXMLASSERT( x )           {}
#endif


#if defined(_MSC_VER) && (_MSC_VER >= 1400 ) && (!defined WINCE)
// Microsoft visual studio, version 2005 and higher.
/*int _snprintf_s(
char *buffer,
size_t sizeOfBuffer,
size_t count,
const char *format [,
argument] ...
);*/
inline int TIXML_SNPRINTF(char* buffer, size_t size, const char* format, ...)
{
  va_list va;
  va_start(va, format);
  int result = vsnprintf_s(buffer, size, _TRUNCATE, format, va);
  va_end(va);
  return result;
}
#define TIXML_SSCANF   sscanf_s
#elif defined WINCE
#define TIXML_SNPRINTF _snprintf
#define TIXML_SSCANF   sscanf
#else
// GCC version 3 and higher
//#warning( "Using sn* functions." )
#define TIXML_SNPRINTF snprintf
#define TIXML_SSCANF   sscanf
#endif

/* Versioning, past 1.0.14:
http://semver.org/
*/
static const int TIXML2_MAJOR_VERSION = 2;
static const int TIXML2_MINOR_VERSION = 2;
static const int TIXML2_PATCH_VERSION = 0;

namespace Aws
{
namespace External
{
namespace tinyxml2
{
  class XMLDocument;
  class XMLElement;
  class XMLAttribute;
  class XMLComment;
  class XMLText;
  class XMLDeclaration;
  class XMLUnknown;
  class XMLPrinter;

  static const char* ALLOCATION_TAG = "AWS::TinyXML";

  /*
  A class that wraps strings. Normally stores the start and end
  pointers into the XML file itself, and will apply normalization
  and entity translation if actually read. Can also store (and memory
  manage) a traditional char[]
  */
  class TINYXML2_LIB StrPair
  {
  public:
    enum {
      NEEDS_ENTITY_PROCESSING = 0x01,
      NEEDS_NEWLINE_NORMALIZATION = 0x02,
      COLLAPSE_WHITESPACE = 0x04,

      TEXT_ELEMENT = NEEDS_ENTITY_PROCESSING | NEEDS_NEWLINE_NORMALIZATION,
      TEXT_ELEMENT_LEAVE_ENTITIES = NEEDS_NEWLINE_NORMALIZATION,
      ATTRIBUTE_NAME = 0,
      ATTRIBUTE_VALUE = NEEDS_ENTITY_PROCESSING | NEEDS_NEWLINE_NORMALIZATION,
      ATTRIBUTE_VALUE_LEAVE_ENTITIES = NEEDS_NEWLINE_NORMALIZATION,
      COMMENT = NEEDS_NEWLINE_NORMALIZATION
    };

    StrPair() : _flags(0), _start(0), _end(0) {}
    ~StrPair();

    void Set(char* start, char* end, int flags) {
      Reset();
      _start = start;
      _end = end;
      _flags = flags | NEEDS_FLUSH;
    }

    const char* GetStr();

    bool Empty() const {
      return _start == _end;
    }

    void SetInternedStr(const char* str) {
      Reset();
      _start = const_cast<char*>(str);
    }

    void SetStr(const char* str, int flags = 0);

    char* ParseText(char* in, const char* endTag, int strFlags);
    char* ParseName(char* in);

    void TransferTo(StrPair* other);

  private:
    void Reset();
    void CollapseWhitespace();

    enum {
      NEEDS_FLUSH = 0x100,
      NEEDS_DELETE = 0x200
    };

    // After parsing, if *_end != 0, it can be set to zero.
    int     _flags;
    char*   _start;
    char*   _end;

    StrPair(const StrPair& other);	// not supported
    void operator=(StrPair& other);	// not supported, use TransferTo()
  };


  /*
  A dynamic array of Plain Old Data. Doesn't support constructors, etc.
  Has a small initial memory pool, so that low or no usage will not
  cause a call to new/delete
  */
  template <class T, int INIT>
  class DynArray
  {
  public:
    DynArray() {
      _mem = _pool;
      _allocated = INIT;
      _size = 0;
    }

    ~DynArray() {
      if (_mem != _pool) {
        Aws::DeleteArray<T>(_mem);
      }
    }

    void Clear() {
      _size = 0;
    }

    void Push(T t) {
      TIXMLASSERT(_size < INT_MAX);
      EnsureCapacity(_size + 1);
      _mem[_size++] = t;
    }

    T* PushArr(int count) {
      TIXMLASSERT(count >= 0);
      TIXMLASSERT(_size <= INT_MAX - count);
      EnsureCapacity(_size + count);
      T* ret = &_mem[_size];
      _size += count;
      return ret;
    }

    T Pop() {
      TIXMLASSERT(_size > 0);
      return _mem[--_size];
    }

    void PopArr(int count) {
      TIXMLASSERT(_size >= count);
      _size -= count;
    }

    bool Empty() const					{
      return _size == 0;
    }

    T& operator[](int i)				{
      TIXMLASSERT(i >= 0 && i < _size);
      return _mem[i];
    }

    const T& operator[](int i) const	{
      TIXMLASSERT(i >= 0 && i < _size);
      return _mem[i];
    }

    const T& PeekTop() const            {
      TIXMLASSERT(_size > 0);
      return _mem[_size - 1];
    }

    int Size() const					{
      TIXMLASSERT(_size >= 0);
      return _size;
    }

    int Capacity() const				{
      return _allocated;
    }

    const T* Mem() const				{
      return _mem;
    }

    T* Mem()							{
      return _mem;
    }

  private:
    DynArray(const DynArray&); // not supported
    void operator=(const DynArray&); // not supported

    void EnsureCapacity(int cap) {
      TIXMLASSERT(cap > 0);
      if (cap > _allocated) {
        TIXMLASSERT(cap <= INT_MAX / 2);
        int newAllocated = cap * 2;
        T* newMem = Aws::NewArray<T>(newAllocated, ALLOCATION_TAG);
        memcpy(newMem, _mem, sizeof(T)*_size);	// warning: not using constructors, only works for PODs
        if (_mem != _pool) {
          Aws::DeleteArray<T>(_mem);
        }
        _mem = newMem;
        _allocated = newAllocated;
      }
    }

    T*  _mem;
    T   _pool[INIT];
    int _allocated;		// objects allocated
    int _size;			// number objects in use
  };


  /*
  Parent virtual class of a pool for fast allocation
  and deallocation of objects.
  */
  class MemPool
  {
  public:
    MemPool() {}
    virtual ~MemPool() {}

    virtual int ItemSize() const = 0;
    virtual void* Alloc() = 0;
    virtual void Free(void*) = 0;
    virtual void SetTracked() = 0;
    virtual void Clear() = 0;
  };


  /*
  Template child class to create pools of the correct type.
  */
  template< int SIZE >
  class MemPoolT : public MemPool
  {
  public:
    MemPoolT() : _root(0), _currentAllocs(0), _nAllocs(0), _maxAllocs(0), _nUntracked(0)	{}
    ~MemPoolT() {
      Clear();
    }

    void Clear() {
      // Delete the blocks.
      while (!_blockPtrs.Empty()) {
        Block* b = _blockPtrs.Pop();
        Aws::Delete(b);
      }
      _root = 0;
      _currentAllocs = 0;
      _nAllocs = 0;
      _maxAllocs = 0;
      _nUntracked = 0;
    }

    virtual int ItemSize() const	{
      return SIZE;
    }
    int CurrentAllocs() const		{
      return _currentAllocs;
    }

    virtual void* Alloc() {
      if (!_root) {
        // Need a new block.
        Block* block = Aws::New<Block>(ALLOCATION_TAG);
        _blockPtrs.Push(block);

        for (int i = 0; i<COUNT - 1; ++i) {
          block->chunk[i].next = &block->chunk[i + 1];
        }
        block->chunk[COUNT - 1].next = 0;
        _root = block->chunk;
      }
      void* result = _root;
      _root = _root->next;

      ++_currentAllocs;
      if (_currentAllocs > _maxAllocs) {
        _maxAllocs = _currentAllocs;
      }
      _nAllocs++;
      _nUntracked++;
      return result;
    }

    virtual void Free(void* mem) {
      if (!mem) {
        return;
      }
      --_currentAllocs;
      Chunk* chunk = static_cast<Chunk*>(mem);
#ifdef DEBUG
      memset(chunk, 0xfe, sizeof(Chunk));
#endif
      chunk->next = _root;
      _root = chunk;
    }
    void Trace(const char* name) {
      printf("Mempool %s watermark=%d [%dk] current=%d size=%d nAlloc=%d blocks=%d\n",
        name, _maxAllocs, _maxAllocs*SIZE / 1024, _currentAllocs, SIZE, _nAllocs, _blockPtrs.Size());
    }

    void SetTracked() {
      _nUntracked--;
    }

    int Untracked() const {
      return _nUntracked;
    }

    // This number is perf sensitive. 4k seems like a good tradeoff on my machine.
    // The test file is large, 170k.
    // Release:		VS2010 gcc(no opt)
    //		1k:		4000
    //		2k:		4000
    //		4k:		3900	21000
    //		16k:	5200
    //		32k:	4300
    //		64k:	4000	21000
    enum { COUNT = (4 * 1024) / SIZE }; // Some compilers do not accept to use COUNT in private part if COUNT is private

  private:
    MemPoolT(const MemPoolT&); // not supported
    void operator=(const MemPoolT&); // not supported

    union Chunk {
      Chunk*  next;
      char    mem[SIZE];
    };
    struct Block {
      Chunk chunk[COUNT];
    };
    DynArray< Block*, 10 > _blockPtrs;
    Chunk* _root;

    int _currentAllocs;
    int _nAllocs;
    int _maxAllocs;
    int _nUntracked;
  };



  /**
  Implements the interface to the "Visitor pattern" (see the Accept() method.)
  If you call the Accept() method, it requires being passed a XMLVisitor
  class to handle callbacks. For nodes that contain other nodes (Document, Element)
  you will get called with a VisitEnter/VisitExit pair. Nodes that are always leafs
  are simply called with Visit().

  If you return 'true' from a Visit method, recursive parsing will continue. If you return
  false, <b>no children of this node or its siblings</b> will be visited.

  All flavors of Visit methods have a default implementation that returns 'true' (continue
  visiting). You need to only override methods that are interesting to you.

  Generally Accept() is called on the XMLDocument, although all nodes support visiting.

  You should never change the document from a callback.

  @sa XMLNode::Accept()
  */
  class TINYXML2_LIB XMLVisitor
  {
  public:
    virtual ~XMLVisitor() {}

    /// Visit a document.
    virtual bool VisitEnter(const XMLDocument& /*doc*/)			{
      return true;
    }
    /// Visit a document.
    virtual bool VisitExit(const XMLDocument& /*doc*/)			{
      return true;
    }

    /// Visit an element.
    virtual bool VisitEnter(const XMLElement& /*element*/, const XMLAttribute* /*firstAttribute*/)	{
      return true;
    }
    /// Visit an element.
    virtual bool VisitExit(const XMLElement& /*element*/)			{
      return true;
    }

    /// Visit a declaration.
    virtual bool Visit(const XMLDeclaration& /*declaration*/)		{
      return true;
    }
    /// Visit a text node.
    virtual bool Visit(const XMLText& /*text*/)					{
      return true;
    }
    /// Visit a comment node.
    virtual bool Visit(const XMLComment& /*comment*/)				{
      return true;
    }
    /// Visit an unknown node.
    virtual bool Visit(const XMLUnknown& /*unknown*/)				{
      return true;
    }
  };

  // WARNING: must match XMLDocument::_errorNames[]
  enum XMLError {
    XML_SUCCESS = 0,
    XML_NO_ERROR = 0,
    XML_NO_ATTRIBUTE,
    XML_WRONG_ATTRIBUTE_TYPE,
    XML_ERROR_FILE_NOT_FOUND,
    XML_ERROR_FILE_COULD_NOT_BE_OPENED,
    XML_ERROR_FILE_READ_ERROR,
    XML_ERROR_ELEMENT_MISMATCH,
    XML_ERROR_PARSING_ELEMENT,
    XML_ERROR_PARSING_ATTRIBUTE,
    XML_ERROR_IDENTIFYING_TAG,
    XML_ERROR_PARSING_TEXT,
    XML_ERROR_PARSING_CDATA,
    XML_ERROR_PARSING_COMMENT,
    XML_ERROR_PARSING_DECLARATION,
    XML_ERROR_PARSING_UNKNOWN,
    XML_ERROR_EMPTY_DOCUMENT,
    XML_ERROR_MISMATCHED_ELEMENT,
    XML_ERROR_PARSING,
    XML_CAN_NOT_CONVERT_TEXT,
    XML_NO_TEXT_NODE,

    XML_ERROR_COUNT
  };


  /*
  Utility functionality.
  */
  class XMLUtil
  {
  public:
    static const char* SkipWhiteSpace(const char* p)	{
      TIXMLASSERT(p);
      while (IsWhiteSpace(*p)) {
        ++p;
      }
      TIXMLASSERT(p);
      return p;
    }
    static char* SkipWhiteSpace(char* p)				{
      return const_cast<char*>(SkipWhiteSpace(const_cast<const char*>(p)));
    }

    // Anything in the high order range of UTF-8 is assumed to not be whitespace. This isn't
    // correct, but simple, and usually works.
    static bool IsWhiteSpace(char p)					{
      return !IsUTF8Continuation(p) && isspace(static_cast<unsigned char>(p));
    }

    inline static bool IsNameStartChar(unsigned char ch) {
      if (ch >= 128) {
        // This is a heuristic guess in attempt to not implement Unicode-aware isalpha()
        return true;
      }
      if (isalpha(ch)) {
        return true;
      }
      return ch == ':' || ch == '_';
    }

    inline static bool IsNameChar(unsigned char ch) {
      return IsNameStartChar(ch)
        || isdigit(ch)
        || ch == '.'
        || ch == '-';
    }

    inline static bool StringEqual(const char* p, const char* q, int nChar = INT_MAX)  {
      if (p == q) {
        return true;
      }
      int n = 0;
      while (*p && *q && *p == *q && n<nChar) {
        ++p;
        ++q;
        ++n;
      }
      if ((n == nChar) || (*p == 0 && *q == 0)) {
        return true;
      }
      return false;
    }

    inline static bool IsUTF8Continuation(const char p) {
      return (p & 0x80) != 0;
    }

    static const char* ReadBOM(const char* p, bool* hasBOM);
    // p is the starting location,
    // the UTF-8 value of the entity will be placed in value, and length filled in.
    static const char* GetCharacterRef(const char* p, char* value, int* length);
    static void ConvertUTF32ToUTF8(unsigned long input, char* output, int* length);

    // converts primitive types to strings
    static void ToStr(int v, char* buffer, int bufferSize);
    static void ToStr(unsigned v, char* buffer, int bufferSize);
    static void ToStr(bool v, char* buffer, int bufferSize);
    static void ToStr(float v, char* buffer, int bufferSize);
    static void ToStr(double v, char* buffer, int bufferSize);

    // converts strings to primitive types
    static bool	ToInt(const char* str, int* value);
    static bool ToUnsigned(const char* str, unsigned* value);
    static bool	ToBool(const char* str, bool* value);
    static bool	ToFloat(const char* str, float* value);
    static bool ToDouble(const char* str, double* value);
  };


  /** XMLNode is a base class for every object that is in the
  XML Document Object Model (DOM), except XMLAttributes.
  Nodes have siblings, a parent, and children which can
  be navigated. A node is always in a XMLDocument.
  The type of a XMLNode can be queried, and it can
  be cast to its more defined type.

  A XMLDocument allocates memory for all its Nodes.
  When the XMLDocument gets deleted, all its Nodes
  will also be deleted.

  @verbatim
  A Document can contain:	Element	(container or leaf)
  Comment (leaf)
  Unknown (leaf)
  Declaration( leaf )

  An Element can contain:	Element (container or leaf)
  Text	(leaf)
  Attributes (not on tree)
  Comment (leaf)
  Unknown (leaf)

  @endverbatim
  */
  class TINYXML2_LIB XMLNode
  {
    friend class XMLDocument;
    friend class XMLElement;
  public:

    /// Get the XMLDocument that owns this XMLNode.
    const XMLDocument* GetDocument() const	{
      return _document;
    }
    /// Get the XMLDocument that owns this XMLNode.
    XMLDocument* GetDocument()				{
      return _document;
    }

    /// Safely cast to an Element, or null.
    virtual XMLElement*		ToElement()		{
      return 0;
    }
    /// Safely cast to Text, or null.
    virtual XMLText*		ToText()		{
      return 0;
    }
    /// Safely cast to a Comment, or null.
    virtual XMLComment*		ToComment()		{
      return 0;
    }
    /// Safely cast to a Document, or null.
    virtual XMLDocument*	ToDocument()	{
      return 0;
    }
    /// Safely cast to a Declaration, or null.
    virtual XMLDeclaration*	ToDeclaration()	{
      return 0;
    }
    /// Safely cast to an Unknown, or null.
    virtual XMLUnknown*		ToUnknown()		{
      return 0;
    }

    virtual const XMLElement*		ToElement() const		{
      return 0;
    }
    virtual const XMLText*			ToText() const			{
      return 0;
    }
    virtual const XMLComment*		ToComment() const		{
      return 0;
    }
    virtual const XMLDocument*		ToDocument() const		{
      return 0;
    }
    virtual const XMLDeclaration*	ToDeclaration() const	{
      return 0;
    }
    virtual const XMLUnknown*		ToUnknown() const		{
      return 0;
    }

    /** The meaning of 'value' changes for the specific type.
    @verbatim
    Document:	empty
    Element:	name of the element
    Comment:	the comment text
    Unknown:	the tag contents
    Text:		the text string
    @endverbatim
    */
    const char* Value() const;

    /** Set the Value of an XML node.
    @sa Value()
    */
    void SetValue(const char* val, bool staticMem = false);

    /// Get the parent of this node on the DOM.
    const XMLNode*	Parent() const			{
      return _parent;
    }

    XMLNode* Parent()						{
      return _parent;
    }

    /// Returns true if this node has no children.
    bool NoChildren() const					{
      return !_firstChild;
    }

    /// Get the first child node, or null if none exists.
    const XMLNode*  FirstChild() const		{
      return _firstChild;
    }

    XMLNode*		FirstChild()			{
      return _firstChild;
    }

    /** Get the first child element, or optionally the first child
    element with the specified name.
    */
    const XMLElement* FirstChildElement(const char* value = 0) const;

    XMLElement* FirstChildElement(const char* value = 0)	{
      return const_cast<XMLElement*>(const_cast<const XMLNode*>(this)->FirstChildElement(value));
    }

    /// Get the last child node, or null if none exists.
    const XMLNode*	LastChild() const						{
      return _lastChild;
    }

    XMLNode*		LastChild()								{
      return const_cast<XMLNode*>(const_cast<const XMLNode*>(this)->LastChild());
    }

    /** Get the last child element or optionally the last child
    element with the specified name.
    */
    const XMLElement* LastChildElement(const char* value = 0) const;

    XMLElement* LastChildElement(const char* value = 0)	{
      return const_cast<XMLElement*>(const_cast<const XMLNode*>(this)->LastChildElement(value));
    }

    /// Get the previous (left) sibling node of this node.
    const XMLNode*	PreviousSibling() const					{
      return _prev;
    }

    XMLNode*	PreviousSibling()							{
      return _prev;
    }

    /// Get the previous (left) sibling element of this node, with an optionally supplied name.
    const XMLElement*	PreviousSiblingElement(const char* value = 0) const;

    XMLElement*	PreviousSiblingElement(const char* value = 0) {
      return const_cast<XMLElement*>(const_cast<const XMLNode*>(this)->PreviousSiblingElement(value));
    }

    /// Get the next (right) sibling node of this node.
    const XMLNode*	NextSibling() const						{
      return _next;
    }

    XMLNode*	NextSibling()								{
      return _next;
    }

    /// Get the next (right) sibling element of this node, with an optionally supplied name.
    const XMLElement*	NextSiblingElement(const char* value = 0) const;

    XMLElement*	NextSiblingElement(const char* value = 0)	{
      return const_cast<XMLElement*>(const_cast<const XMLNode*>(this)->NextSiblingElement(value));
    }

    /**
    Add a child node as the last (right) child.
    If the child node is already part of the document,
    it is moved from its old location to the new location.
    Returns the addThis argument or 0 if the node does not
    belong to the same document.
    */
    XMLNode* InsertEndChild(XMLNode* addThis);

    XMLNode* LinkEndChild(XMLNode* addThis)	{
      return InsertEndChild(addThis);
    }
    /**
    Add a child node as the first (left) child.
    If the child node is already part of the document,
    it is moved from its old location to the new location.
    Returns the addThis argument or 0 if the node does not
    belong to the same document.
    */
    XMLNode* InsertFirstChild(XMLNode* addThis);
    /**
    Add a node after the specified child node.
    If the child node is already part of the document,
    it is moved from its old location to the new location.
    Returns the addThis argument or 0 if the afterThis node
    is not a child of this node, or if the node does not
    belong to the same document.
    */
    XMLNode* InsertAfterChild(XMLNode* afterThis, XMLNode* addThis);

    /**
    Delete all the children of this node.
    */
    void DeleteChildren();

    /**
    Delete a child of this node.
    */
    void DeleteChild(XMLNode* node);

    /**
    Make a copy of this node, but not its children.
    You may pass in a Document pointer that will be
    the owner of the new Node. If the 'document' is
    null, then the node returned will be allocated
    from the current Document. (this->GetDocument())

    Note: if called on a XMLDocument, this will return null.
    */
    virtual XMLNode* ShallowClone(XMLDocument* document) const = 0;

    /**
    Test if 2 nodes are the same, but don't test children.
    The 2 nodes do not need to be in the same Document.

    Note: if called on a XMLDocument, this will return false.
    */
    virtual bool ShallowEqual(const XMLNode* compare) const = 0;

    /** Accept a hierarchical visit of the nodes in the TinyXML-2 DOM. Every node in the
    XML tree will be conditionally visited and the host will be called back
    via the XMLVisitor interface.

    This is essentially a SAX interface for TinyXML-2. (Note however it doesn't re-parse
    the XML for the callbacks, so the performance of TinyXML-2 is unchanged by using this
    interface versus any other.)

    The interface has been based on ideas from:

    - http://www.saxproject.org/
    - http://c2.com/cgi/wiki?HierarchicalVisitorPattern

    Which are both good references for "visiting".

    An example of using Accept():
    @verbatim
    XMLPrinter printer;
    tinyxmlDoc.Accept( &printer );
    const char* xmlcstr = printer.CStr();
    @endverbatim
    */
    virtual bool Accept(XMLVisitor* visitor) const = 0;

    // internal
    virtual char* ParseDeep(char*, StrPair*);

  protected:
    XMLNode(XMLDocument*);
    virtual ~XMLNode();

    XMLDocument*	_document;
    XMLNode*		_parent;
    mutable StrPair	_value;

    XMLNode*		_firstChild;
    XMLNode*		_lastChild;

    XMLNode*		_prev;
    XMLNode*		_next;

  private:
    MemPool*		_memPool;
    void Unlink(XMLNode* child);
    static void DeleteNode(XMLNode* node);
    void InsertChildPreamble(XMLNode* insertThis) const;

    XMLNode(const XMLNode&);	// not supported
    XMLNode& operator=(const XMLNode&);	// not supported
  };


  /** XML text.

  Note that a text node can have child element nodes, for example:
  @verbatim
  <root>This is <b>bold</b></root>
  @endverbatim

  A text node can have 2 ways to output the next. "normal" output
  and CDATA. It will default to the mode it was parsed from the XML file and
  you generally want to leave it alone, but you can change the output mode with
  SetCData() and query it with CData().
  */
  class TINYXML2_LIB XMLText : public XMLNode
  {
    friend class XMLBase;
    friend class XMLDocument;
  public:
    virtual bool Accept(XMLVisitor* visitor) const;

    virtual XMLText* ToText()			{
      return this;
    }
    virtual const XMLText* ToText() const	{
      return this;
    }

    /// Declare whether this should be CDATA or standard text.
    void SetCData(bool isCData)			{
      _isCData = isCData;
    }
    /// Returns true if this is a CDATA text element.
    bool CData() const						{
      return _isCData;
    }

    char* ParseDeep(char*, StrPair* endTag);
    virtual XMLNode* ShallowClone(XMLDocument* document) const;
    virtual bool ShallowEqual(const XMLNode* compare) const;

  protected:
    XMLText(XMLDocument* doc) : XMLNode(doc), _isCData(false)	{}
    virtual ~XMLText()												{}

  private:
    bool _isCData;

    XMLText(const XMLText&);	// not supported
    XMLText& operator=(const XMLText&);	// not supported
  };


  /** An XML Comment. */
  class TINYXML2_LIB XMLComment : public XMLNode
  {
    friend class XMLDocument;
  public:
    virtual XMLComment*	ToComment()					{
      return this;
    }
    virtual const XMLComment* ToComment() const		{
      return this;
    }

    virtual bool Accept(XMLVisitor* visitor) const;

    char* ParseDeep(char*, StrPair* endTag);
    virtual XMLNode* ShallowClone(XMLDocument* document) const;
    virtual bool ShallowEqual(const XMLNode* compare) const;

  protected:
    XMLComment(XMLDocument* doc);
    virtual ~XMLComment();

  private:
    XMLComment(const XMLComment&);	// not supported
    XMLComment& operator=(const XMLComment&);	// not supported
  };


  /** In correct XML the declaration is the first entry in the file.
  @verbatim
  <?xml version="1.0" standalone="yes"?>
  @endverbatim

  TinyXML-2 will happily read or write files without a declaration,
  however.

  The text of the declaration isn't interpreted. It is parsed
  and written as a string.
  */
  class TINYXML2_LIB XMLDeclaration : public XMLNode
  {
    friend class XMLDocument;
  public:
    virtual XMLDeclaration*	ToDeclaration()					{
      return this;
    }
    virtual const XMLDeclaration* ToDeclaration() const		{
      return this;
    }

    virtual bool Accept(XMLVisitor* visitor) const;

    char* ParseDeep(char*, StrPair* endTag);
    virtual XMLNode* ShallowClone(XMLDocument* document) const;
    virtual bool ShallowEqual(const XMLNode* compare) const;

  protected:
    XMLDeclaration(XMLDocument* doc);
    virtual ~XMLDeclaration();

  private:
    XMLDeclaration(const XMLDeclaration&);	// not supported
    XMLDeclaration& operator=(const XMLDeclaration&);	// not supported
  };


  /** Any tag that TinyXML-2 doesn't recognize is saved as an
  unknown. It is a tag of text, but should not be modified.
  It will be written back to the XML, unchanged, when the file
  is saved.

  DTD tags get thrown into XMLUnknowns.
  */
  class TINYXML2_LIB XMLUnknown : public XMLNode
  {
    friend class XMLDocument;
  public:
    virtual XMLUnknown*	ToUnknown()					{
      return this;
    }
    virtual const XMLUnknown* ToUnknown() const		{
      return this;
    }

    virtual bool Accept(XMLVisitor* visitor) const;

    char* ParseDeep(char*, StrPair* endTag);
    virtual XMLNode* ShallowClone(XMLDocument* document) const;
    virtual bool ShallowEqual(const XMLNode* compare) const;

  protected:
    XMLUnknown(XMLDocument* doc);
    virtual ~XMLUnknown();

  private:
    XMLUnknown(const XMLUnknown&);	// not supported
    XMLUnknown& operator=(const XMLUnknown&);	// not supported
  };



  /** An attribute is a name-value pair. Elements have an arbitrary
  number of attributes, each with a unique name.

  @note The attributes are not XMLNodes. You may only query the
  Next() attribute in a list.
  */
  class TINYXML2_LIB XMLAttribute
  {
    friend class XMLElement;
  public:
    /// The name of the attribute.
    const char* Name() const;

    /// The value of the attribute.
    const char* Value() const;

    /// The next attribute in the list.
    const XMLAttribute* Next() const {
      return _next;
    }

    /** IntValue interprets the attribute as an integer, and returns the value.
    If the value isn't an integer, 0 will be returned. There is no error checking;
    use QueryIntValue() if you need error checking.
    */
    int		 IntValue() const				{
      int i = 0;
      QueryIntValue(&i);
      return i;
    }
    /// Query as an unsigned integer. See IntValue()
    unsigned UnsignedValue() const			{
      unsigned i = 0;
      QueryUnsignedValue(&i);
      return i;
    }
    /// Query as a boolean. See IntValue()
    bool	 BoolValue() const				{
      bool b = false;
      QueryBoolValue(&b);
      return b;
    }
    /// Query as a double. See IntValue()
    double 	 DoubleValue() const			{
      double d = 0;
      QueryDoubleValue(&d);
      return d;
    }
    /// Query as a float. See IntValue()
    float	 FloatValue() const				{
      float f = 0;
      QueryFloatValue(&f);
      return f;
    }

    /** QueryIntValue interprets the attribute as an integer, and returns the value
    in the provided parameter. The function will return XML_NO_ERROR on success,
    and XML_WRONG_ATTRIBUTE_TYPE if the conversion is not successful.
    */
    XMLError QueryIntValue(int* value) const;
    /// See QueryIntValue
    XMLError QueryUnsignedValue(unsigned int* value) const;
    /// See QueryIntValue
    XMLError QueryBoolValue(bool* value) const;
    /// See QueryIntValue
    XMLError QueryDoubleValue(double* value) const;
    /// See QueryIntValue
    XMLError QueryFloatValue(float* value) const;

    /// Set the attribute to a string value.
    void SetAttribute(const char* value);
    /// Set the attribute to value.
    void SetAttribute(int value);
    /// Set the attribute to value.
    void SetAttribute(unsigned value);
    /// Set the attribute to value.
    void SetAttribute(bool value);
    /// Set the attribute to value.
    void SetAttribute(double value);
    /// Set the attribute to value.
    void SetAttribute(float value);

  private:
    enum { BUF_SIZE = 200 };

    XMLAttribute() : _next(0), _memPool(0) {}
    virtual ~XMLAttribute()	{}

    XMLAttribute(const XMLAttribute&);	// not supported
    void operator=(const XMLAttribute&);	// not supported
    void SetName(const char* name);

    char* ParseDeep(char* p, bool processEntities);

    mutable StrPair _name;
    mutable StrPair _value;
    XMLAttribute*   _next;
    MemPool*        _memPool;
  };


  /** The element is a container class. It has a value, the element name,
  and can contain other elements, text, comments, and unknowns.
  Elements also contain an arbitrary number of attributes.
  */
  class TINYXML2_LIB XMLElement : public XMLNode
  {
    friend class XMLBase;
    friend class XMLDocument;
  public:
    /// Get the name of an element (which is the Value() of the node.)
    const char* Name() const		{
      return Value();
    }
    /// Set the name of the element.
    void SetName(const char* str, bool staticMem = false)	{
      SetValue(str, staticMem);
    }

    virtual XMLElement* ToElement()				{
      return this;
    }
    virtual const XMLElement* ToElement() const {
      return this;
    }
    virtual bool Accept(XMLVisitor* visitor) const;

    /** Given an attribute name, Attribute() returns the value
    for the attribute of that name, or null if none
    exists. For example:

    @verbatim
    const char* value = ele->Attribute( "foo" );
    @endverbatim

    The 'value' parameter is normally null. However, if specified,
    the attribute will only be returned if the 'name' and 'value'
    match. This allow you to write code:

    @verbatim
    if ( ele->Attribute( "foo", "bar" ) ) callFooIsBar();
    @endverbatim

    rather than:
    @verbatim
    if ( ele->Attribute( "foo" ) ) {
    if ( strcmp( ele->Attribute( "foo" ), "bar" ) == 0 ) callFooIsBar();
    }
    @endverbatim
    */
    const char* Attribute(const char* name, const char* value = 0) const;

    /** Given an attribute name, IntAttribute() returns the value
    of the attribute interpreted as an integer. 0 will be
    returned if there is an error. For a method with error
    checking, see QueryIntAttribute()
    */
    int		 IntAttribute(const char* name) const		{
      int i = 0;
      QueryIntAttribute(name, &i);
      return i;
    }
    /// See IntAttribute()
    unsigned UnsignedAttribute(const char* name) const {
      unsigned i = 0;
      QueryUnsignedAttribute(name, &i);
      return i;
    }
    /// See IntAttribute()
    bool	 BoolAttribute(const char* name) const	{
      bool b = false;
      QueryBoolAttribute(name, &b);
      return b;
    }
    /// See IntAttribute()
    double 	 DoubleAttribute(const char* name) const	{
      double d = 0;
      QueryDoubleAttribute(name, &d);
      return d;
    }
    /// See IntAttribute()
    float	 FloatAttribute(const char* name) const	{
      float f = 0;
      QueryFloatAttribute(name, &f);
      return f;
    }

    /** Given an attribute name, QueryIntAttribute() returns
    XML_NO_ERROR, XML_WRONG_ATTRIBUTE_TYPE if the conversion
    can't be performed, or XML_NO_ATTRIBUTE if the attribute
    doesn't exist. If successful, the result of the conversion
    will be written to 'value'. If not successful, nothing will
    be written to 'value'. This allows you to provide default
    value:

    @verbatim
    int value = 10;
    QueryIntAttribute( "foo", &value );		// if "foo" isn't found, value will still be 10
    @endverbatim
    */
    XMLError QueryIntAttribute(const char* name, int* value) const				{
      const XMLAttribute* a = FindAttribute(name);
      if (!a) {
        return XML_NO_ATTRIBUTE;
      }
      return a->QueryIntValue(value);
    }
    /// See QueryIntAttribute()
    XMLError QueryUnsignedAttribute(const char* name, unsigned int* value) const	{
      const XMLAttribute* a = FindAttribute(name);
      if (!a) {
        return XML_NO_ATTRIBUTE;
      }
      return a->QueryUnsignedValue(value);
    }
    /// See QueryIntAttribute()
    XMLError QueryBoolAttribute(const char* name, bool* value) const				{
      const XMLAttribute* a = FindAttribute(name);
      if (!a) {
        return XML_NO_ATTRIBUTE;
      }
      return a->QueryBoolValue(value);
    }
    /// See QueryIntAttribute()
    XMLError QueryDoubleAttribute(const char* name, double* value) const			{
      const XMLAttribute* a = FindAttribute(name);
      if (!a) {
        return XML_NO_ATTRIBUTE;
      }
      return a->QueryDoubleValue(value);
    }
    /// See QueryIntAttribute()
    XMLError QueryFloatAttribute(const char* name, float* value) const			{
      const XMLAttribute* a = FindAttribute(name);
      if (!a) {
        return XML_NO_ATTRIBUTE;
      }
      return a->QueryFloatValue(value);
    }


    /** Given an attribute name, QueryAttribute() returns
    XML_NO_ERROR, XML_WRONG_ATTRIBUTE_TYPE if the conversion
    can't be performed, or XML_NO_ATTRIBUTE if the attribute
    doesn't exist. It is overloaded for the primitive types,
    and is a generally more convenient replacement of
    QueryIntAttribute() and related functions.

    If successful, the result of the conversion
    will be written to 'value'. If not successful, nothing will
    be written to 'value'. This allows you to provide default
    value:

    @verbatim
    int value = 10;
    QueryAttribute( "foo", &value );		// if "foo" isn't found, value will still be 10
    @endverbatim
    */
    int QueryAttribute(const char* name, int* value) const {
      return QueryIntAttribute(name, value);
    }

    int QueryAttribute(const char* name, unsigned int* value) const {
      return QueryUnsignedAttribute(name, value);
    }

    int QueryAttribute(const char* name, bool* value) const {
      return QueryBoolAttribute(name, value);
    }

    int QueryAttribute(const char* name, double* value) const {
      return QueryDoubleAttribute(name, value);
    }

    int QueryAttribute(const char* name, float* value) const {
      return QueryFloatAttribute(name, value);
    }

    /// Sets the named attribute to value.
    void SetAttribute(const char* name, const char* value)	{
      XMLAttribute* a = FindOrCreateAttribute(name);
      a->SetAttribute(value);
    }
    /// Sets the named attribute to value.
    void SetAttribute(const char* name, int value)			{
      XMLAttribute* a = FindOrCreateAttribute(name);
      a->SetAttribute(value);
    }
    /// Sets the named attribute to value.
    void SetAttribute(const char* name, unsigned value)		{
      XMLAttribute* a = FindOrCreateAttribute(name);
      a->SetAttribute(value);
    }
    /// Sets the named attribute to value.
    void SetAttribute(const char* name, bool value)			{
      XMLAttribute* a = FindOrCreateAttribute(name);
      a->SetAttribute(value);
    }
    /// Sets the named attribute to value.
    void SetAttribute(const char* name, double value)		{
      XMLAttribute* a = FindOrCreateAttribute(name);
      a->SetAttribute(value);
    }
    /// Sets the named attribute to value.
    void SetAttribute(const char* name, float value)		{
      XMLAttribute* a = FindOrCreateAttribute(name);
      a->SetAttribute(value);
    }

    /**
    Delete an attribute.
    */
    void DeleteAttribute(const char* name);

    /// Return the first attribute in the list.
    const XMLAttribute* FirstAttribute() const {
      return _rootAttribute;
    }
    /// Query a specific attribute in the list.
    const XMLAttribute* FindAttribute(const char* name) const;

    /** Convenience function for easy access to the text inside an element. Although easy
    and concise, GetText() is limited compared to getting the XMLText child
    and accessing it directly.

    If the first child of 'this' is a XMLText, the GetText()
    returns the character string of the Text node, else null is returned.

    This is a convenient method for getting the text of simple contained text:
    @verbatim
    <foo>This is text</foo>
    const char* str = fooElement->GetText();
    @endverbatim

    'str' will be a pointer to "This is text".

    Note that this function can be misleading. If the element foo was created from
    this XML:
    @verbatim
    <foo><b>This is text</b></foo>
    @endverbatim

    then the value of str would be null. The first child node isn't a text node, it is
    another element. From this XML:
    @verbatim
    <foo>This is <b>text</b></foo>
    @endverbatim
    GetText() will return "This is ".
    */
    const char* GetText() const;

    /** Convenience function for easy access to the text inside an element. Although easy
    and concise, SetText() is limited compared to creating an XMLText child
    and mutating it directly.

    If the first child of 'this' is a XMLText, SetText() sets its value to
    the given string, otherwise it will create a first child that is an XMLText.

    This is a convenient method for setting the text of simple contained text:
    @verbatim
    <foo>This is text</foo>
    fooElement->SetText( "Hullaballoo!" );
    <foo>Hullaballoo!</foo>
    @endverbatim

    Note that this function can be misleading. If the element foo was created from
    this XML:
    @verbatim
    <foo><b>This is text</b></foo>
    @endverbatim

    then it will not change "This is text", but rather prefix it with a text element:
    @verbatim
    <foo>Hullaballoo!<b>This is text</b></foo>
    @endverbatim

    For this XML:
    @verbatim
    <foo />
    @endverbatim
    SetText() will generate
    @verbatim
    <foo>Hullaballoo!</foo>
    @endverbatim
    */
    void SetText(const char* inText);
    /// Convenience method for setting text inside and element. See SetText() for important limitations.
    void SetText(int value);
    /// Convenience method for setting text inside and element. See SetText() for important limitations.
    void SetText(unsigned value);
    /// Convenience method for setting text inside and element. See SetText() for important limitations.
    void SetText(bool value);
    /// Convenience method for setting text inside and element. See SetText() for important limitations.
    void SetText(double value);
    /// Convenience method for setting text inside and element. See SetText() for important limitations.
    void SetText(float value);

    /**
    Convenience method to query the value of a child text node. This is probably best
    shown by example. Given you have a document is this form:
    @verbatim
    <point>
    <x>1</x>
    <y>1.4</y>
    </point>
    @endverbatim

    The QueryIntText() and similar functions provide a safe and easier way to get to the
    "value" of x and y.

    @verbatim
    int x = 0;
    float y = 0;	// types of x and y are contrived for example
    const XMLElement* xElement = pointElement->FirstChildElement( "x" );
    const XMLElement* yElement = pointElement->FirstChildElement( "y" );
    xElement->QueryIntText( &x );
    yElement->QueryFloatText( &y );
    @endverbatim

    @returns XML_SUCCESS (0) on success, XML_CAN_NOT_CONVERT_TEXT if the text cannot be converted
    to the requested type, and XML_NO_TEXT_NODE if there is no child text to query.

    */
    XMLError QueryIntText(int* ival) const;
    /// See QueryIntText()
    XMLError QueryUnsignedText(unsigned* uval) const;
    /// See QueryIntText()
    XMLError QueryBoolText(bool* bval) const;
    /// See QueryIntText()
    XMLError QueryDoubleText(double* dval) const;
    /// See QueryIntText()
    XMLError QueryFloatText(float* fval) const;

    // internal:
    enum {
      OPEN,		// <foo>
      CLOSED,		// <foo/>
      CLOSING		// </foo>
    };
    int ClosingType() const {
      return _closingType;
    }
    char* ParseDeep(char* p, StrPair* endTag);
    virtual XMLNode* ShallowClone(XMLDocument* document) const;
    virtual bool ShallowEqual(const XMLNode* compare) const;

  private:
    XMLElement(XMLDocument* doc);
    virtual ~XMLElement();
    XMLElement(const XMLElement&);	// not supported
    void operator=(const XMLElement&);	// not supported

    XMLAttribute* FindAttribute(const char* name) {
      return const_cast<XMLAttribute*>(const_cast<const XMLElement*>(this)->FindAttribute(name));
    }
    XMLAttribute* FindOrCreateAttribute(const char* name);
    //void LinkAttribute( XMLAttribute* attrib );
    char* ParseAttributes(char* p);
    static void DeleteAttribute(XMLAttribute* attribute);

    enum { BUF_SIZE = 200 };
    int _closingType;
    // The attribute list is ordered; there is no 'lastAttribute'
    // because the list needs to be scanned for dupes before adding
    // a new attribute.
    XMLAttribute* _rootAttribute;
  };


  enum Whitespace {
    PRESERVE_WHITESPACE,
    COLLAPSE_WHITESPACE
  };


  /** A Document binds together all the functionality.
  It can be saved, loaded, and printed to the screen.
  All Nodes are connected and allocated to a Document.
  If the Document is deleted, all its Nodes are also deleted.
  */
  class TINYXML2_LIB XMLDocument : public XMLNode
  {
    friend class XMLElement;
  public:
    /// constructor
    XMLDocument(bool processEntities = true, Whitespace = PRESERVE_WHITESPACE);
    ~XMLDocument();

    virtual XMLDocument* ToDocument()				{
      return this;
    }
    virtual const XMLDocument* ToDocument() const	{
      return this;
    }

    /**
    Parse an XML file from a character string.
    Returns XML_NO_ERROR (0) on success, or
    an errorID.

    You may optionally pass in the 'nBytes', which is
    the number of bytes which will be parsed. If not
    specified, TinyXML-2 will assume 'xml' points to a
    null terminated string.
    */
    XMLError Parse(const char* xml, size_t nBytes = (size_t)(-1));

    /**
    Load an XML file from disk.
    Returns XML_NO_ERROR (0) on success, or
    an errorID.
    */
    XMLError LoadFile(const char* filename);

    /**
    Load an XML file from disk. You are responsible
    for providing and closing the FILE*.

    NOTE: The file should be opened as binary ("rb")
    not text in order for TinyXML-2 to correctly
    do newline normalization.

    Returns XML_NO_ERROR (0) on success, or
    an errorID.
    */
    XMLError LoadFile(FILE*);

    /**
    Save the XML file to disk.
    Returns XML_NO_ERROR (0) on success, or
    an errorID.
    */
    XMLError SaveFile(const char* filename, bool compact = false);

    /**
    Save the XML file to disk. You are responsible
    for providing and closing the FILE*.

    Returns XML_NO_ERROR (0) on success, or
    an errorID.
    */
    XMLError SaveFile(FILE* fp, bool compact = false);

    bool ProcessEntities() const		{
      return _processEntities;
    }
    Whitespace WhitespaceMode() const	{
      return _whitespace;
    }

    /**
    Returns true if this document has a leading Byte Order Mark of UTF8.
    */
    bool HasBOM() const {
      return _writeBOM;
    }
    /** Sets whether to write the BOM when writing the file.
    */
    void SetBOM(bool useBOM) {
      _writeBOM = useBOM;
    }

    /** Return the root element of DOM. Equivalent to FirstChildElement().
    To get the first node, use FirstChild().
    */
    XMLElement* RootElement()				{
      return FirstChildElement();
    }
    const XMLElement* RootElement() const	{
      return FirstChildElement();
    }

    /** Print the Document. If the Printer is not provided, it will
    print to stdout. If you provide Printer, this can print to a file:
    @verbatim
    XMLPrinter printer( fp );
    doc.Print( &printer );
    @endverbatim

    Or you can use a printer to print to memory:
    @verbatim
    XMLPrinter printer;
    doc.Print( &printer );
    // printer.CStr() has a const char* to the XML
    @endverbatim
    */
    void Print(XMLPrinter* streamer = 0) const;
    virtual bool Accept(XMLVisitor* visitor) const;

    /**
    Create a new Element associated with
    this Document. The memory for the Element
    is managed by the Document.
    */
    XMLElement* NewElement(const char* name);
    /**
    Create a new Comment associated with
    this Document. The memory for the Comment
    is managed by the Document.
    */
    XMLComment* NewComment(const char* comment);
    /**
    Create a new Text associated with
    this Document. The memory for the Text
    is managed by the Document.
    */
    XMLText* NewText(const char* text);
    /**
    Create a new Declaration associated with
    this Document. The memory for the object
    is managed by the Document.

    If the 'text' param is null, the standard
    declaration is used.:
    @verbatim
    <?xml version="1.0" encoding="UTF-8"?>
    @endverbatim
    */
    XMLDeclaration* NewDeclaration(const char* text = 0);
    /**
    Create a new Unknown associated with
    this Document. The memory for the object
    is managed by the Document.
    */
    XMLUnknown* NewUnknown(const char* text);

    /**
    Delete a node associated with this document.
    It will be unlinked from the DOM.
    */
    void DeleteNode(XMLNode* node);

    void SetError(XMLError error, const char* str1, const char* str2);

    /// Return true if there was an error parsing the document.
    bool Error() const {
      return _errorID != XML_NO_ERROR;
    }
    /// Return the errorID.
    XMLError  ErrorID() const {
      return _errorID;
    }
    const char* ErrorName() const;

    /// Return a possibly helpful diagnostic location or string.
    const char* GetErrorStr1() const {
      return _errorStr1;
    }
    /// Return a possibly helpful secondary diagnostic location or string.
    const char* GetErrorStr2() const {
      return _errorStr2;
    }
    /// If there is an error, print it to stdout.
    void PrintError() const;

    /// Clear the document, resetting it to the initial state.
    void Clear();

    // internal
    char* Identify(char* p, XMLNode** node);

    virtual XMLNode* ShallowClone(XMLDocument* /*document*/) const	{
      return 0;
    }
    virtual bool ShallowEqual(const XMLNode* /*compare*/) const	{
      return false;
    }

  private:
    XMLDocument(const XMLDocument&);	// not supported
    void operator=(const XMLDocument&);	// not supported

    bool        _writeBOM;
    bool        _processEntities;
    XMLError    _errorID;
    Whitespace  _whitespace;
    const char* _errorStr1;
    const char* _errorStr2;
    char*       _charBuffer;

    MemPoolT< sizeof(XMLElement) >	 _elementPool;
    MemPoolT< sizeof(XMLAttribute) > _attributePool;
    MemPoolT< sizeof(XMLText) >		 _textPool;
    MemPoolT< sizeof(XMLComment) >	 _commentPool;

    static const char* _errorNames[XML_ERROR_COUNT];

    void Parse();
  };


  /**
  A XMLHandle is a class that wraps a node pointer with null checks; this is
  an incredibly useful thing. Note that XMLHandle is not part of the TinyXML-2
  DOM structure. It is a separate utility class.

  Take an example:
  @verbatim
  <Document>
  <Element attributeA = "valueA">
  <Child attributeB = "value1" />
  <Child attributeB = "value2" />
  </Element>
  </Document>
  @endverbatim

  Assuming you want the value of "attributeB" in the 2nd "Child" element, it's very
  easy to write a *lot* of code that looks like:

  @verbatim
  XMLElement* root = document.FirstChildElement( "Document" );
  if ( root )
  {
  XMLElement* element = root->FirstChildElement( "Element" );
  if ( element )
  {
  XMLElement* child = element->FirstChildElement( "Child" );
  if ( child )
  {
  XMLElement* child2 = child->NextSiblingElement( "Child" );
  if ( child2 )
  {
  // Finally do something useful.
  @endverbatim

  And that doesn't even cover "else" cases. XMLHandle addresses the verbosity
  of such code. A XMLHandle checks for null pointers so it is perfectly safe
  and correct to use:

  @verbatim
  XMLHandle docHandle( &document );
  XMLElement* child2 = docHandle.FirstChildElement( "Document" ).FirstChildElement( "Element" ).FirstChildElement().NextSiblingElement();
  if ( child2 )
  {
  // do something useful
  @endverbatim

  Which is MUCH more concise and useful.

  It is also safe to copy handles - internally they are nothing more than node pointers.
  @verbatim
  XMLHandle handleCopy = handle;
  @endverbatim

  See also XMLConstHandle, which is the same as XMLHandle, but operates on const objects.
  */
  class TINYXML2_LIB XMLHandle
  {
  public:
    /// Create a handle from any node (at any depth of the tree.) This can be a null pointer.
    XMLHandle(XMLNode* node)												{
      _node = node;
    }
    /// Create a handle from a node.
    XMLHandle(XMLNode& node)												{
      _node = &node;
    }
    /// Copy constructor
    XMLHandle(const XMLHandle& ref)										{
      _node = ref._node;
    }
    /// Assignment
    XMLHandle& operator=(const XMLHandle& ref)							{
      _node = ref._node;
      return *this;
    }

    /// Get the first child of this handle.
    XMLHandle FirstChild() 													{
      return XMLHandle(_node ? _node->FirstChild() : 0);
    }
    /// Get the first child element of this handle.
    XMLHandle FirstChildElement(const char* value = 0)						{
      return XMLHandle(_node ? _node->FirstChildElement(value) : 0);
    }
    /// Get the last child of this handle.
    XMLHandle LastChild()													{
      return XMLHandle(_node ? _node->LastChild() : 0);
    }
    /// Get the last child element of this handle.
    XMLHandle LastChildElement(const char* _value = 0)						{
      return XMLHandle(_node ? _node->LastChildElement(_value) : 0);
    }
    /// Get the previous sibling of this handle.
    XMLHandle PreviousSibling()												{
      return XMLHandle(_node ? _node->PreviousSibling() : 0);
    }
    /// Get the previous sibling element of this handle.
    XMLHandle PreviousSiblingElement(const char* _value = 0)				{
      return XMLHandle(_node ? _node->PreviousSiblingElement(_value) : 0);
    }
    /// Get the next sibling of this handle.
    XMLHandle NextSibling()													{
      return XMLHandle(_node ? _node->NextSibling() : 0);
    }
    /// Get the next sibling element of this handle.
    XMLHandle NextSiblingElement(const char* _value = 0)					{
      return XMLHandle(_node ? _node->NextSiblingElement(_value) : 0);
    }

    /// Safe cast to XMLNode. This can return null.
    XMLNode* ToNode()							{
      return _node;
    }
    /// Safe cast to XMLElement. This can return null.
    XMLElement* ToElement() 					{
      return ((_node == 0) ? 0 : _node->ToElement());
    }
    /// Safe cast to XMLText. This can return null.
    XMLText* ToText() 							{
      return ((_node == 0) ? 0 : _node->ToText());
    }
    /// Safe cast to XMLUnknown. This can return null.
    XMLUnknown* ToUnknown() 					{
      return ((_node == 0) ? 0 : _node->ToUnknown());
    }
    /// Safe cast to XMLDeclaration. This can return null.
    XMLDeclaration* ToDeclaration() 			{
      return ((_node == 0) ? 0 : _node->ToDeclaration());
    }

  private:
    XMLNode* _node;
  };


  /**
  A variant of the XMLHandle class for working with const XMLNodes and Documents. It is the
  same in all regards, except for the 'const' qualifiers. See XMLHandle for API.
  */
  class TINYXML2_LIB XMLConstHandle
  {
  public:
    XMLConstHandle(const XMLNode* node)											{
      _node = node;
    }
    XMLConstHandle(const XMLNode& node)											{
      _node = &node;
    }
    XMLConstHandle(const XMLConstHandle& ref)										{
      _node = ref._node;
    }

    XMLConstHandle& operator=(const XMLConstHandle& ref)							{
      _node = ref._node;
      return *this;
    }

    const XMLConstHandle FirstChild() const											{
      return XMLConstHandle(_node ? _node->FirstChild() : 0);
    }
    const XMLConstHandle FirstChildElement(const char* value = 0) const				{
      return XMLConstHandle(_node ? _node->FirstChildElement(value) : 0);
    }
    const XMLConstHandle LastChild()	const										{
      return XMLConstHandle(_node ? _node->LastChild() : 0);
    }
    const XMLConstHandle LastChildElement(const char* _value = 0) const				{
      return XMLConstHandle(_node ? _node->LastChildElement(_value) : 0);
    }
    const XMLConstHandle PreviousSibling() const									{
      return XMLConstHandle(_node ? _node->PreviousSibling() : 0);
    }
    const XMLConstHandle PreviousSiblingElement(const char* _value = 0) const		{
      return XMLConstHandle(_node ? _node->PreviousSiblingElement(_value) : 0);
    }
    const XMLConstHandle NextSibling() const										{
      return XMLConstHandle(_node ? _node->NextSibling() : 0);
    }
    const XMLConstHandle NextSiblingElement(const char* _value = 0) const			{
      return XMLConstHandle(_node ? _node->NextSiblingElement(_value) : 0);
    }


    const XMLNode* ToNode() const				{
      return _node;
    }
    const XMLElement* ToElement() const			{
      return ((_node == 0) ? 0 : _node->ToElement());
    }
    const XMLText* ToText() const				{
      return ((_node == 0) ? 0 : _node->ToText());
    }
    const XMLUnknown* ToUnknown() const			{
      return ((_node == 0) ? 0 : _node->ToUnknown());
    }
    const XMLDeclaration* ToDeclaration() const	{
      return ((_node == 0) ? 0 : _node->ToDeclaration());
    }

  private:
    const XMLNode* _node;
  };


  /**
  Printing functionality. The XMLPrinter gives you more
  options than the XMLDocument::Print() method.

  It can:
  -# Print to memory.
  -# Print to a file you provide.
  -# Print XML without a XMLDocument.

  Print to Memory

  @verbatim
  XMLPrinter printer;
  doc.Print( &printer );
  SomeFunction( printer.CStr() );
  @endverbatim

  Print to a File

  You provide the file pointer.
  @verbatim
  XMLPrinter printer( fp );
  doc.Print( &printer );
  @endverbatim

  Print without a XMLDocument

  When loading, an XML parser is very useful. However, sometimes
  when saving, it just gets in the way. The code is often set up
  for streaming, and constructing the DOM is just overhead.

  The Printer supports the streaming case. The following code
  prints out a trivially simple XML file without ever creating
  an XML document.

  @verbatim
  XMLPrinter printer( fp );
  printer.OpenElement( "foo" );
  printer.PushAttribute( "foo", "bar" );
  printer.CloseElement();
  @endverbatim
  */
  class TINYXML2_LIB XMLPrinter : public XMLVisitor
  {
  public:
    /** Construct the printer. If the FILE* is specified,
    this will print to the FILE. Else it will print
    to memory, and the result is available in CStr().
    If 'compact' is set to true, then output is created
    with only required whitespace and newlines.
    */
    XMLPrinter(FILE* file = 0, bool compact = false, int depth = 0);
    virtual ~XMLPrinter()	{}

    /** If streaming, write the BOM and declaration. */
    void PushHeader(bool writeBOM, bool writeDeclaration);
    /** If streaming, start writing an element.
    The element must be closed with CloseElement()
    */
    void OpenElement(const char* name, bool compactMode = false);
    /// If streaming, add an attribute to an open element.
    void PushAttribute(const char* name, const char* value);
    void PushAttribute(const char* name, int value);
    void PushAttribute(const char* name, unsigned value);
    void PushAttribute(const char* name, bool value);
    void PushAttribute(const char* name, double value);
    /// If streaming, close the Element.
    virtual void CloseElement(bool compactMode = false);

    /// Add a text node.
    void PushText(const char* text, bool cdata = false);
    /// Add a text node from an integer.
    void PushText(int value);
    /// Add a text node from an unsigned.
    void PushText(unsigned value);
    /// Add a text node from a bool.
    void PushText(bool value);
    /// Add a text node from a float.
    void PushText(float value);
    /// Add a text node from a double.
    void PushText(double value);

    /// Add a comment
    void PushComment(const char* comment);

    void PushDeclaration(const char* value);
    void PushUnknown(const char* value);

    virtual bool VisitEnter(const XMLDocument& /*doc*/);
    virtual bool VisitExit(const XMLDocument& /*doc*/)			{
      return true;
    }

    virtual bool VisitEnter(const XMLElement& element, const XMLAttribute* attribute);
    virtual bool VisitExit(const XMLElement& element);

    virtual bool Visit(const XMLText& text);
    virtual bool Visit(const XMLComment& comment);
    virtual bool Visit(const XMLDeclaration& declaration);
    virtual bool Visit(const XMLUnknown& unknown);

    /**
    If in print to memory mode, return a pointer to
    the XML file in memory.
    */
    const char* CStr() const {
      return _buffer.Mem();
    }
    /**
    If in print to memory mode, return the size
    of the XML file in memory. (Note the size returned
    includes the terminating null.)
    */
    int CStrSize() const {
      return _buffer.Size();
    }
    /**
    If in print to memory mode, reset the buffer to the
    beginning.
    */
    void ClearBuffer() {
      _buffer.Clear();
      _buffer.Push(0);
    }

  protected:
    virtual bool CompactMode(const XMLElement&)	{ return _compactMode; }

    /** Prints out the space before an element. You may override to change
    the space and tabs used. A PrintSpace() override should call Print().
    */
    virtual void PrintSpace(int depth);
    void Print(const char* format, ...);

    void SealElementIfJustOpened();
    bool _elementJustOpened;
    DynArray< const char*, 10 > _stack;

  private:
    void PrintString(const char*, bool restrictedEntitySet);	// prints out, after detecting entities.

    bool _firstElement;
    FILE* _fp;
    int _depth;
    int _textDepth;
    bool _processEntities;
    bool _compactMode;

    enum {
      ENTITY_RANGE = 64,
      BUF_SIZE = 200
    };
    bool _entityFlag[ENTITY_RANGE];
    bool _restrictedEntityFlag[ENTITY_RANGE];

    DynArray< char, 20 > _buffer;
  };


}	// tinyxml2
}   // namespace External
}   // namespace Aws

#if defined(_MSC_VER)
#   pragma warning(pop)
#endif

#endif // TINYXML2_INCLUDED
