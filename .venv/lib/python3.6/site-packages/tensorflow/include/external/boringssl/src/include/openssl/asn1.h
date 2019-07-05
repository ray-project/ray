/* Copyright (C) 1995-1998 Eric Young (eay@cryptsoft.com)
 * All rights reserved.
 *
 * This package is an SSL implementation written
 * by Eric Young (eay@cryptsoft.com).
 * The implementation was written so as to conform with Netscapes SSL.
 * 
 * This library is free for commercial and non-commercial use as long as
 * the following conditions are aheared to.  The following conditions
 * apply to all code found in this distribution, be it the RC4, RSA,
 * lhash, DES, etc., code; not just the SSL code.  The SSL documentation
 * included with this distribution is covered by the same copyright terms
 * except that the holder is Tim Hudson (tjh@cryptsoft.com).
 * 
 * Copyright remains Eric Young's, and as such any Copyright notices in
 * the code are not to be removed.
 * If this package is used in a product, Eric Young should be given attribution
 * as the author of the parts of the library used.
 * This can be in the form of a textual message at program startup or
 * in documentation (online or textual) provided with the package.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software
 *    must display the following acknowledgement:
 *    "This product includes cryptographic software written by
 *     Eric Young (eay@cryptsoft.com)"
 *    The word 'cryptographic' can be left out if the rouines from the library
 *    being used are not cryptographic related :-).
 * 4. If you include any Windows specific code (or a derivative thereof) from 
 *    the apps directory (application code) you must include an acknowledgement:
 *    "This product includes software written by Tim Hudson (tjh@cryptsoft.com)"
 * 
 * THIS SOFTWARE IS PROVIDED BY ERIC YOUNG ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * 
 * The licence and distribution terms for any publically available version or
 * derivative of this code cannot be changed.  i.e. this code cannot simply be
 * copied and put under another distribution licence
 * [including the GNU Public Licence.]
 */

#ifndef HEADER_ASN1_H
#define HEADER_ASN1_H

#include <openssl/base.h>

#include <time.h>

#include <openssl/bio.h>
#include <openssl/stack.h>

#include <openssl/bn.h>

#ifdef  __cplusplus
extern "C" {
#endif


/* Legacy ASN.1 library.
 *
 * This header is part of OpenSSL's ASN.1 implementation. It is retained for
 * compatibility but otherwise underdocumented and not actively maintained. Use
 * the new |CBS| and |CBB| library in <openssl/bytestring.h> instead. */


#define V_ASN1_UNIVERSAL		0x00
#define	V_ASN1_APPLICATION		0x40
#define V_ASN1_CONTEXT_SPECIFIC		0x80
#define V_ASN1_PRIVATE			0xc0

#define V_ASN1_CONSTRUCTED		0x20
#define V_ASN1_PRIMITIVE_TAG		0x1f

#define V_ASN1_APP_CHOOSE		-2	/* let the recipient choose */
#define V_ASN1_OTHER			-3	/* used in ASN1_TYPE */
#define V_ASN1_ANY			-4	/* used in ASN1 template code */

#define V_ASN1_NEG			0x100	/* negative flag */
/* No supported universal tags may exceed this value, to avoid ambiguity with
 * V_ASN1_NEG. */
#define V_ASN1_MAX_UNIVERSAL		0xff

#define V_ASN1_UNDEF			-1
#define V_ASN1_EOC			0
#define V_ASN1_BOOLEAN			1	/**/
#define V_ASN1_INTEGER			2
#define V_ASN1_NEG_INTEGER		(2 | V_ASN1_NEG)
#define V_ASN1_BIT_STRING		3
#define V_ASN1_OCTET_STRING		4
#define V_ASN1_NULL			5
#define V_ASN1_OBJECT			6
#define V_ASN1_OBJECT_DESCRIPTOR	7
#define V_ASN1_EXTERNAL			8
#define V_ASN1_REAL			9
#define V_ASN1_ENUMERATED		10
#define V_ASN1_NEG_ENUMERATED		(10 | V_ASN1_NEG)
#define V_ASN1_UTF8STRING		12
#define V_ASN1_SEQUENCE			16
#define V_ASN1_SET			17
#define V_ASN1_NUMERICSTRING		18	/**/
#define V_ASN1_PRINTABLESTRING		19
#define V_ASN1_T61STRING		20
#define V_ASN1_TELETEXSTRING		20	/* alias */
#define V_ASN1_VIDEOTEXSTRING		21	/**/
#define V_ASN1_IA5STRING		22
#define V_ASN1_UTCTIME			23
#define V_ASN1_GENERALIZEDTIME		24	/**/
#define V_ASN1_GRAPHICSTRING		25	/**/
#define V_ASN1_ISO64STRING		26	/**/
#define V_ASN1_VISIBLESTRING		26	/* alias */
#define V_ASN1_GENERALSTRING		27	/**/
#define V_ASN1_UNIVERSALSTRING		28	/**/
#define V_ASN1_BMPSTRING		30

/* For use with d2i_ASN1_type_bytes() */
#define B_ASN1_NUMERICSTRING	0x0001
#define B_ASN1_PRINTABLESTRING	0x0002
#define B_ASN1_T61STRING	0x0004
#define B_ASN1_TELETEXSTRING	0x0004
#define B_ASN1_VIDEOTEXSTRING	0x0008
#define B_ASN1_IA5STRING	0x0010
#define B_ASN1_GRAPHICSTRING	0x0020
#define B_ASN1_ISO64STRING	0x0040
#define B_ASN1_VISIBLESTRING	0x0040
#define B_ASN1_GENERALSTRING	0x0080
#define B_ASN1_UNIVERSALSTRING	0x0100
#define B_ASN1_OCTET_STRING	0x0200
#define B_ASN1_BIT_STRING	0x0400
#define B_ASN1_BMPSTRING	0x0800
#define B_ASN1_UNKNOWN		0x1000
#define B_ASN1_UTF8STRING	0x2000
#define B_ASN1_UTCTIME		0x4000
#define B_ASN1_GENERALIZEDTIME	0x8000
#define B_ASN1_SEQUENCE		0x10000

/* For use with ASN1_mbstring_copy() */
#define MBSTRING_FLAG		0x1000
#define MBSTRING_UTF8		(MBSTRING_FLAG)
/* |MBSTRING_ASC| refers to Latin-1, not ASCII. It is used with TeletexString
 * which, in turn, is treated as Latin-1 rather than T.61 by OpenSSL and most
 * other software. */
#define MBSTRING_ASC		(MBSTRING_FLAG|1)
#define MBSTRING_BMP		(MBSTRING_FLAG|2)
#define MBSTRING_UNIV		(MBSTRING_FLAG|4)

#define DECLARE_ASN1_SET_OF(type) /* filled in by mkstack.pl */
#define IMPLEMENT_ASN1_SET_OF(type) /* nothing, no longer needed */

/* These are used internally in the ASN1_OBJECT to keep track of
 * whether the names and data need to be free()ed */
#define ASN1_OBJECT_FLAG_DYNAMIC	 0x01	/* internal use */
#define ASN1_OBJECT_FLAG_DYNAMIC_STRINGS 0x04	/* internal use */
#define ASN1_OBJECT_FLAG_DYNAMIC_DATA 	 0x08	/* internal use */
struct asn1_object_st
	{
	const char *sn,*ln;
	int nid;
	int length;
	const unsigned char *data;	/* data remains const after init */
	int flags;	/* Should we free this one */
	};

DEFINE_STACK_OF(ASN1_OBJECT)

#define ASN1_STRING_FLAG_BITS_LEFT 0x08 /* Set if 0x07 has bits left value */
/* This indicates that the ASN1_STRING is not a real value but just a place
 * holder for the location where indefinite length constructed data should
 * be inserted in the memory buffer 
 */
#define ASN1_STRING_FLAG_NDEF 0x010 

/* This flag is used by ASN1 code to indicate an ASN1_STRING is an MSTRING
 * type.
 */
#define ASN1_STRING_FLAG_MSTRING 0x040 
/* This is the base type that holds just about everything :-) */
struct asn1_string_st
	{
	int length;
	int type;
	unsigned char *data;
	/* The value of the following field depends on the type being
	 * held.  It is mostly being used for BIT_STRING so if the
	 * input data has a non-zero 'unused bits' value, it will be
	 * handled correctly */
	long flags;
	};

/* ASN1_ENCODING structure: this is used to save the received
 * encoding of an ASN1 type. This is useful to get round
 * problems with invalid encodings which can break signatures.
 */

typedef struct ASN1_ENCODING_st
	{
	unsigned char *enc;	/* DER encoding */
	long len;		/* Length of encoding */
	int modified;		/* set to 1 if 'enc' is invalid */
	/* alias_only is zero if |enc| owns the buffer that it points to
	 * (although |enc| may still be NULL). If one, |enc| points into a
	 * buffer that is owned elsewhere. */
	unsigned alias_only:1;
	/* alias_only_on_next_parse is one iff the next parsing operation
	 * should avoid taking a copy of the input and rather set
	 * |alias_only|. */
	unsigned alias_only_on_next_parse:1;
	} ASN1_ENCODING;

#define STABLE_FLAGS_MALLOC	0x01
#define STABLE_NO_MASK		0x02
#define DIRSTRING_TYPE	\
 (B_ASN1_PRINTABLESTRING|B_ASN1_T61STRING|B_ASN1_BMPSTRING|B_ASN1_UTF8STRING)
#define PKCS9STRING_TYPE (DIRSTRING_TYPE|B_ASN1_IA5STRING)

typedef struct asn1_string_table_st {
	int nid;
	long minsize;
	long maxsize;
	unsigned long mask;
	unsigned long flags;
} ASN1_STRING_TABLE;

/* size limits: this stuff is taken straight from RFC2459 */

#define ub_name				32768
#define ub_common_name			64
#define ub_locality_name		128
#define ub_state_name			128
#define ub_organization_name		64
#define ub_organization_unit_name	64
#define ub_title			64
#define ub_email_address		128

/* Declarations for template structures: for full definitions
 * see asn1t.h
 */
typedef struct ASN1_TEMPLATE_st ASN1_TEMPLATE;
typedef struct ASN1_TLC_st ASN1_TLC;
/* This is just an opaque pointer */
typedef struct ASN1_VALUE_st ASN1_VALUE;

/* Declare ASN1 functions: the implement macro in in asn1t.h */

#define DECLARE_ASN1_FUNCTIONS(type) DECLARE_ASN1_FUNCTIONS_name(type, type)

#define DECLARE_ASN1_ALLOC_FUNCTIONS(type) \
	DECLARE_ASN1_ALLOC_FUNCTIONS_name(type, type)

#define DECLARE_ASN1_FUNCTIONS_name(type, name) \
	DECLARE_ASN1_ALLOC_FUNCTIONS_name(type, name) \
	DECLARE_ASN1_ENCODE_FUNCTIONS(type, name, name)

#define DECLARE_ASN1_FUNCTIONS_fname(type, itname, name) \
	DECLARE_ASN1_ALLOC_FUNCTIONS_name(type, name) \
	DECLARE_ASN1_ENCODE_FUNCTIONS(type, itname, name)

#define	DECLARE_ASN1_ENCODE_FUNCTIONS(type, itname, name) \
	OPENSSL_EXPORT type *d2i_##name(type **a, const unsigned char **in, long len); \
	OPENSSL_EXPORT int i2d_##name(type *a, unsigned char **out); \
	DECLARE_ASN1_ITEM(itname)

#define	DECLARE_ASN1_ENCODE_FUNCTIONS_const(type, name) \
	OPENSSL_EXPORT type *d2i_##name(type **a, const unsigned char **in, long len); \
	OPENSSL_EXPORT int i2d_##name(const type *a, unsigned char **out); \
	DECLARE_ASN1_ITEM(name)

#define	DECLARE_ASN1_NDEF_FUNCTION(name) \
	OPENSSL_EXPORT int i2d_##name##_NDEF(name *a, unsigned char **out);

#define DECLARE_ASN1_FUNCTIONS_const(name) \
	DECLARE_ASN1_ALLOC_FUNCTIONS(name) \
	DECLARE_ASN1_ENCODE_FUNCTIONS_const(name, name)

#define DECLARE_ASN1_ALLOC_FUNCTIONS_name(type, name) \
	OPENSSL_EXPORT type *name##_new(void); \
	OPENSSL_EXPORT void name##_free(type *a);

#define DECLARE_ASN1_PRINT_FUNCTION(stname) \
	DECLARE_ASN1_PRINT_FUNCTION_fname(stname, stname)

#define DECLARE_ASN1_PRINT_FUNCTION_fname(stname, fname) \
	OPENSSL_EXPORT int fname##_print_ctx(BIO *out, stname *x, int indent, \
					 const ASN1_PCTX *pctx);

#define D2I_OF(type) type *(*)(type **,const unsigned char **,long)
#define I2D_OF(type) int (*)(type *,unsigned char **)
#define I2D_OF_const(type) int (*)(const type *,unsigned char **)

#define CHECKED_D2I_OF(type, d2i) \
    ((d2i_of_void*) (1 ? d2i : ((D2I_OF(type))0)))
#define CHECKED_I2D_OF(type, i2d) \
    ((i2d_of_void*) (1 ? i2d : ((I2D_OF(type))0)))
#define CHECKED_NEW_OF(type, xnew) \
    ((void *(*)(void)) (1 ? xnew : ((type *(*)(void))0)))
#define CHECKED_PPTR_OF(type, p) \
    ((void**) (1 ? p : (type**)0))

typedef void *d2i_of_void(void **, const unsigned char **, long);
typedef int i2d_of_void(const void *, unsigned char **);

/* The following macros and typedefs allow an ASN1_ITEM
 * to be embedded in a structure and referenced. Since
 * the ASN1_ITEM pointers need to be globally accessible
 * (possibly from shared libraries) they may exist in
 * different forms. On platforms that support it the
 * ASN1_ITEM structure itself will be globally exported.
 * Other platforms will export a function that returns
 * an ASN1_ITEM pointer.
 *
 * To handle both cases transparently the macros below
 * should be used instead of hard coding an ASN1_ITEM
 * pointer in a structure.
 *
 * The structure will look like this:
 *
 * typedef struct SOMETHING_st {
 *      ...
 *      ASN1_ITEM_EXP *iptr;
 *      ...
 * } SOMETHING; 
 *
 * It would be initialised as e.g.:
 *
 * SOMETHING somevar = {...,ASN1_ITEM_ref(X509),...};
 *
 * and the actual pointer extracted with:
 *
 * const ASN1_ITEM *it = ASN1_ITEM_ptr(somevar.iptr);
 *
 * Finally an ASN1_ITEM pointer can be extracted from an
 * appropriate reference with: ASN1_ITEM_rptr(X509). This
 * would be used when a function takes an ASN1_ITEM * argument.
 *
 */

/* ASN1_ITEM pointer exported type */
typedef const ASN1_ITEM ASN1_ITEM_EXP;

/* Macro to obtain ASN1_ITEM pointer from exported type */
#define ASN1_ITEM_ptr(iptr) (iptr)

/* Macro to include ASN1_ITEM pointer from base type */
#define ASN1_ITEM_ref(iptr) (&(iptr##_it))

#define ASN1_ITEM_rptr(ref) (&(ref##_it))

#define DECLARE_ASN1_ITEM(name) \
	extern OPENSSL_EXPORT const ASN1_ITEM name##_it;

/* Parameters used by ASN1_STRING_print_ex() */

/* These determine which characters to escape:
 * RFC2253 special characters, control characters and
 * MSB set characters
 */

#define ASN1_STRFLGS_ESC_2253		1
#define ASN1_STRFLGS_ESC_CTRL		2
#define ASN1_STRFLGS_ESC_MSB		4


/* This flag determines how we do escaping: normally
 * RC2253 backslash only, set this to use backslash and
 * quote.
 */

#define ASN1_STRFLGS_ESC_QUOTE		8


/* These three flags are internal use only. */

/* Character is a valid PrintableString character */
#define CHARTYPE_PRINTABLESTRING	0x10
/* Character needs escaping if it is the first character */
#define CHARTYPE_FIRST_ESC_2253		0x20
/* Character needs escaping if it is the last character */
#define CHARTYPE_LAST_ESC_2253		0x40

/* NB the internal flags are safely reused below by flags
 * handled at the top level.
 */

/* If this is set we convert all character strings
 * to UTF8 first 
 */

#define ASN1_STRFLGS_UTF8_CONVERT	0x10

/* If this is set we don't attempt to interpret content:
 * just assume all strings are 1 byte per character. This
 * will produce some pretty odd looking output!
 */

#define ASN1_STRFLGS_IGNORE_TYPE	0x20

/* If this is set we include the string type in the output */
#define ASN1_STRFLGS_SHOW_TYPE		0x40

/* This determines which strings to display and which to
 * 'dump' (hex dump of content octets or DER encoding). We can
 * only dump non character strings or everything. If we
 * don't dump 'unknown' they are interpreted as character
 * strings with 1 octet per character and are subject to
 * the usual escaping options.
 */

#define ASN1_STRFLGS_DUMP_ALL		0x80
#define ASN1_STRFLGS_DUMP_UNKNOWN	0x100

/* These determine what 'dumping' does, we can dump the
 * content octets or the DER encoding: both use the
 * RFC2253 #XXXXX notation.
 */

#define ASN1_STRFLGS_DUMP_DER		0x200

/* All the string flags consistent with RFC2253,
 * escaping control characters isn't essential in
 * RFC2253 but it is advisable anyway.
 */

#define ASN1_STRFLGS_RFC2253	(ASN1_STRFLGS_ESC_2253 | \
				ASN1_STRFLGS_ESC_CTRL | \
				ASN1_STRFLGS_ESC_MSB | \
				ASN1_STRFLGS_UTF8_CONVERT | \
				ASN1_STRFLGS_DUMP_UNKNOWN | \
				ASN1_STRFLGS_DUMP_DER)

DEFINE_STACK_OF(ASN1_INTEGER)
DECLARE_ASN1_SET_OF(ASN1_INTEGER)

struct asn1_type_st
	{
	int type;
	union	{
		char *ptr;
		ASN1_BOOLEAN		boolean;
		ASN1_STRING *		asn1_string;
		ASN1_OBJECT *		object;
		ASN1_INTEGER *		integer;
		ASN1_ENUMERATED *	enumerated;
		ASN1_BIT_STRING *	bit_string;
		ASN1_OCTET_STRING *	octet_string;
		ASN1_PRINTABLESTRING *	printablestring;
		ASN1_T61STRING *	t61string;
		ASN1_IA5STRING *	ia5string;
		ASN1_GENERALSTRING *	generalstring;
		ASN1_BMPSTRING *	bmpstring;
		ASN1_UNIVERSALSTRING *	universalstring;
		ASN1_UTCTIME *		utctime;
		ASN1_GENERALIZEDTIME *	generalizedtime;
		ASN1_VISIBLESTRING *	visiblestring;
		ASN1_UTF8STRING *	utf8string;
		/* set and sequence are left complete and still
		 * contain the set or sequence bytes */
		ASN1_STRING *		set;
		ASN1_STRING *		sequence;
		ASN1_VALUE *		asn1_value;
		} value;
    };

DEFINE_STACK_OF(ASN1_TYPE)
DECLARE_ASN1_SET_OF(ASN1_TYPE)

typedef STACK_OF(ASN1_TYPE) ASN1_SEQUENCE_ANY;

DECLARE_ASN1_ENCODE_FUNCTIONS_const(ASN1_SEQUENCE_ANY, ASN1_SEQUENCE_ANY)
DECLARE_ASN1_ENCODE_FUNCTIONS_const(ASN1_SEQUENCE_ANY, ASN1_SET_ANY)

struct X509_algor_st
       {
       ASN1_OBJECT *algorithm;
       ASN1_TYPE *parameter;
       } /* X509_ALGOR */;

DECLARE_ASN1_FUNCTIONS(X509_ALGOR)

/* This is used to contain a list of bit names */
typedef struct BIT_STRING_BITNAME_st {
	int bitnum;
	const char *lname;
	const char *sname;
} BIT_STRING_BITNAME;


#define M_ASN1_STRING_length(x)	((x)->length)
#define M_ASN1_STRING_length_set(x, n)	((x)->length = (n))
#define M_ASN1_STRING_type(x)	((x)->type)
#define M_ASN1_STRING_data(x)	((x)->data)

/* Macros for string operations */
#define M_ASN1_BIT_STRING_new()	(ASN1_BIT_STRING *)\
		ASN1_STRING_type_new(V_ASN1_BIT_STRING)
#define M_ASN1_BIT_STRING_free(a)	ASN1_STRING_free((ASN1_STRING *)a)
#define M_ASN1_BIT_STRING_dup(a) (ASN1_BIT_STRING *)\
		ASN1_STRING_dup((const ASN1_STRING *)a)
#define M_ASN1_BIT_STRING_cmp(a,b) ASN1_STRING_cmp(\
		(const ASN1_STRING *)a,(const ASN1_STRING *)b)
#define M_ASN1_BIT_STRING_set(a,b,c) ASN1_STRING_set((ASN1_STRING *)a,b,c)

#define M_ASN1_INTEGER_new()	(ASN1_INTEGER *)\
		ASN1_STRING_type_new(V_ASN1_INTEGER)
#define M_ASN1_INTEGER_free(a)		ASN1_STRING_free((ASN1_STRING *)a)
#define M_ASN1_INTEGER_dup(a) (ASN1_INTEGER *)\
		ASN1_STRING_dup((const ASN1_STRING *)a)
#define M_ASN1_INTEGER_cmp(a,b)	ASN1_STRING_cmp(\
		(const ASN1_STRING *)a,(const ASN1_STRING *)b)

#define M_ASN1_ENUMERATED_new()	(ASN1_ENUMERATED *)\
		ASN1_STRING_type_new(V_ASN1_ENUMERATED)
#define M_ASN1_ENUMERATED_free(a)	ASN1_STRING_free((ASN1_STRING *)a)
#define M_ASN1_ENUMERATED_dup(a) (ASN1_ENUMERATED *)\
		ASN1_STRING_dup((const ASN1_STRING *)a)
#define M_ASN1_ENUMERATED_cmp(a,b)	ASN1_STRING_cmp(\
		(const ASN1_STRING *)a,(const ASN1_STRING *)b)

#define M_ASN1_OCTET_STRING_new()	(ASN1_OCTET_STRING *)\
		ASN1_STRING_type_new(V_ASN1_OCTET_STRING)
#define M_ASN1_OCTET_STRING_free(a)	ASN1_STRING_free((ASN1_STRING *)a)
#define M_ASN1_OCTET_STRING_dup(a) (ASN1_OCTET_STRING *)\
		ASN1_STRING_dup((const ASN1_STRING *)a)
#define M_ASN1_OCTET_STRING_cmp(a,b) ASN1_STRING_cmp(\
		(const ASN1_STRING *)a,(const ASN1_STRING *)b)
#define M_ASN1_OCTET_STRING_set(a,b,c)	ASN1_STRING_set((ASN1_STRING *)a,b,c)
#define M_ASN1_OCTET_STRING_print(a,b)	ASN1_STRING_print(a,(ASN1_STRING *)b)

#define B_ASN1_TIME \
			B_ASN1_UTCTIME | \
			B_ASN1_GENERALIZEDTIME

#define B_ASN1_PRINTABLE \
			B_ASN1_NUMERICSTRING| \
			B_ASN1_PRINTABLESTRING| \
			B_ASN1_T61STRING| \
			B_ASN1_IA5STRING| \
			B_ASN1_BIT_STRING| \
			B_ASN1_UNIVERSALSTRING|\
			B_ASN1_BMPSTRING|\
			B_ASN1_UTF8STRING|\
			B_ASN1_SEQUENCE|\
			B_ASN1_UNKNOWN

#define B_ASN1_DIRECTORYSTRING \
			B_ASN1_PRINTABLESTRING| \
			B_ASN1_TELETEXSTRING|\
			B_ASN1_BMPSTRING|\
			B_ASN1_UNIVERSALSTRING|\
			B_ASN1_UTF8STRING

#define B_ASN1_DISPLAYTEXT \
			B_ASN1_IA5STRING| \
			B_ASN1_VISIBLESTRING| \
			B_ASN1_BMPSTRING|\
			B_ASN1_UTF8STRING

#define M_ASN1_PRINTABLE_new()	ASN1_STRING_type_new(V_ASN1_T61STRING)
#define M_ASN1_PRINTABLE_free(a)	ASN1_STRING_free((ASN1_STRING *)a)

#define M_DIRECTORYSTRING_new() ASN1_STRING_type_new(V_ASN1_PRINTABLESTRING)
#define M_DIRECTORYSTRING_free(a)	ASN1_STRING_free((ASN1_STRING *)a)

#define M_DISPLAYTEXT_new() ASN1_STRING_type_new(V_ASN1_VISIBLESTRING)
#define M_DISPLAYTEXT_free(a) ASN1_STRING_free((ASN1_STRING *)a)

#define M_ASN1_PRINTABLESTRING_new() (ASN1_PRINTABLESTRING *)\
		ASN1_STRING_type_new(V_ASN1_PRINTABLESTRING)
#define M_ASN1_PRINTABLESTRING_free(a)	ASN1_STRING_free((ASN1_STRING *)a)

#define M_ASN1_T61STRING_new()	(ASN1_T61STRING *)\
		ASN1_STRING_type_new(V_ASN1_T61STRING)
#define M_ASN1_T61STRING_free(a)	ASN1_STRING_free((ASN1_STRING *)a)

#define M_ASN1_IA5STRING_new()	(ASN1_IA5STRING *)\
		ASN1_STRING_type_new(V_ASN1_IA5STRING)
#define M_ASN1_IA5STRING_free(a)	ASN1_STRING_free((ASN1_STRING *)a)
#define M_ASN1_IA5STRING_dup(a)	\
		(ASN1_IA5STRING *)ASN1_STRING_dup((const ASN1_STRING *)a)

#define M_ASN1_UTCTIME_new()	(ASN1_UTCTIME *)\
		ASN1_STRING_type_new(V_ASN1_UTCTIME)
#define M_ASN1_UTCTIME_free(a)	ASN1_STRING_free((ASN1_STRING *)a)
#define M_ASN1_UTCTIME_dup(a) (ASN1_UTCTIME *)\
		ASN1_STRING_dup((const ASN1_STRING *)a)

#define M_ASN1_GENERALIZEDTIME_new()	(ASN1_GENERALIZEDTIME *)\
		ASN1_STRING_type_new(V_ASN1_GENERALIZEDTIME)
#define M_ASN1_GENERALIZEDTIME_free(a)	ASN1_STRING_free((ASN1_STRING *)a)
#define M_ASN1_GENERALIZEDTIME_dup(a) (ASN1_GENERALIZEDTIME *)ASN1_STRING_dup(\
	(const ASN1_STRING *)a)

#define M_ASN1_TIME_new()	(ASN1_TIME *)\
		ASN1_STRING_type_new(V_ASN1_UTCTIME)
#define M_ASN1_TIME_free(a)	ASN1_STRING_free((ASN1_STRING *)a)
#define M_ASN1_TIME_dup(a) (ASN1_TIME *)\
	ASN1_STRING_dup((const ASN1_STRING *)a)

#define M_ASN1_GENERALSTRING_new()	(ASN1_GENERALSTRING *)\
		ASN1_STRING_type_new(V_ASN1_GENERALSTRING)
#define M_ASN1_GENERALSTRING_free(a)	ASN1_STRING_free((ASN1_STRING *)a)

#define M_ASN1_UNIVERSALSTRING_new()	(ASN1_UNIVERSALSTRING *)\
		ASN1_STRING_type_new(V_ASN1_UNIVERSALSTRING)
#define M_ASN1_UNIVERSALSTRING_free(a)	ASN1_STRING_free((ASN1_STRING *)a)

#define M_ASN1_BMPSTRING_new()	(ASN1_BMPSTRING *)\
		ASN1_STRING_type_new(V_ASN1_BMPSTRING)
#define M_ASN1_BMPSTRING_free(a)	ASN1_STRING_free((ASN1_STRING *)a)

#define M_ASN1_VISIBLESTRING_new()	(ASN1_VISIBLESTRING *)\
		ASN1_STRING_type_new(V_ASN1_VISIBLESTRING)
#define M_ASN1_VISIBLESTRING_free(a)	ASN1_STRING_free((ASN1_STRING *)a)

#define M_ASN1_UTF8STRING_new()	(ASN1_UTF8STRING *)\
		ASN1_STRING_type_new(V_ASN1_UTF8STRING)
#define M_ASN1_UTF8STRING_free(a)	ASN1_STRING_free((ASN1_STRING *)a)

DECLARE_ASN1_FUNCTIONS_fname(ASN1_TYPE, ASN1_ANY, ASN1_TYPE)

OPENSSL_EXPORT int ASN1_TYPE_get(ASN1_TYPE *a);
OPENSSL_EXPORT void ASN1_TYPE_set(ASN1_TYPE *a, int type, void *value);
OPENSSL_EXPORT int ASN1_TYPE_set1(ASN1_TYPE *a, int type, const void *value);
OPENSSL_EXPORT int ASN1_TYPE_cmp(const ASN1_TYPE *a, const ASN1_TYPE *b);

OPENSSL_EXPORT ASN1_OBJECT *	ASN1_OBJECT_new(void );
OPENSSL_EXPORT void		ASN1_OBJECT_free(ASN1_OBJECT *a);
OPENSSL_EXPORT int		i2d_ASN1_OBJECT(ASN1_OBJECT *a,unsigned char **pp);
OPENSSL_EXPORT ASN1_OBJECT *	c2i_ASN1_OBJECT(ASN1_OBJECT **a,const unsigned char **pp,
						long length);
OPENSSL_EXPORT ASN1_OBJECT *	d2i_ASN1_OBJECT(ASN1_OBJECT **a,const unsigned char **pp,
						long length);

DECLARE_ASN1_ITEM(ASN1_OBJECT)

DECLARE_ASN1_SET_OF(ASN1_OBJECT)

OPENSSL_EXPORT ASN1_STRING *	ASN1_STRING_new(void);
OPENSSL_EXPORT void		ASN1_STRING_free(ASN1_STRING *a);
OPENSSL_EXPORT int		ASN1_STRING_copy(ASN1_STRING *dst, const ASN1_STRING *str);
OPENSSL_EXPORT ASN1_STRING *	ASN1_STRING_dup(const ASN1_STRING *a);
OPENSSL_EXPORT ASN1_STRING *	ASN1_STRING_type_new(int type );
OPENSSL_EXPORT int 		ASN1_STRING_cmp(const ASN1_STRING *a, const ASN1_STRING *b);
  /* Since this is used to store all sorts of things, via macros, for now, make
     its data void * */
OPENSSL_EXPORT int 		ASN1_STRING_set(ASN1_STRING *str, const void *data, int len);
OPENSSL_EXPORT void		ASN1_STRING_set0(ASN1_STRING *str, void *data, int len);
OPENSSL_EXPORT int ASN1_STRING_length(const ASN1_STRING *x);
OPENSSL_EXPORT void ASN1_STRING_length_set(ASN1_STRING *x, int n);
OPENSSL_EXPORT int ASN1_STRING_type(ASN1_STRING *x);
OPENSSL_EXPORT unsigned char * ASN1_STRING_data(ASN1_STRING *x);
OPENSSL_EXPORT const unsigned char *ASN1_STRING_get0_data(const ASN1_STRING *x);

DECLARE_ASN1_FUNCTIONS(ASN1_BIT_STRING)
OPENSSL_EXPORT int		i2c_ASN1_BIT_STRING(ASN1_BIT_STRING *a,unsigned char **pp);
OPENSSL_EXPORT ASN1_BIT_STRING *c2i_ASN1_BIT_STRING(ASN1_BIT_STRING **a,const unsigned char **pp, long length);
OPENSSL_EXPORT int		ASN1_BIT_STRING_set(ASN1_BIT_STRING *a, unsigned char *d, int length );
OPENSSL_EXPORT int		ASN1_BIT_STRING_set_bit(ASN1_BIT_STRING *a, int n, int value);
OPENSSL_EXPORT int		ASN1_BIT_STRING_get_bit(ASN1_BIT_STRING *a, int n);
OPENSSL_EXPORT int            ASN1_BIT_STRING_check(ASN1_BIT_STRING *a, unsigned char *flags, int flags_len);

OPENSSL_EXPORT int		i2d_ASN1_BOOLEAN(int a,unsigned char **pp);
OPENSSL_EXPORT int 		d2i_ASN1_BOOLEAN(int *a,const unsigned char **pp,long length);

DECLARE_ASN1_FUNCTIONS(ASN1_INTEGER)
OPENSSL_EXPORT int		i2c_ASN1_INTEGER(ASN1_INTEGER *a,unsigned char **pp);
OPENSSL_EXPORT ASN1_INTEGER *c2i_ASN1_INTEGER(ASN1_INTEGER **a,const unsigned char **pp, long length);
OPENSSL_EXPORT ASN1_INTEGER *d2i_ASN1_UINTEGER(ASN1_INTEGER **a,const unsigned char **pp, long length);
OPENSSL_EXPORT ASN1_INTEGER *	ASN1_INTEGER_dup(const ASN1_INTEGER *x);
OPENSSL_EXPORT int ASN1_INTEGER_cmp(const ASN1_INTEGER *x, const ASN1_INTEGER *y);

DECLARE_ASN1_FUNCTIONS(ASN1_ENUMERATED)

OPENSSL_EXPORT int ASN1_UTCTIME_check(const ASN1_UTCTIME *a);
OPENSSL_EXPORT ASN1_UTCTIME *ASN1_UTCTIME_set(ASN1_UTCTIME *s,time_t t);
OPENSSL_EXPORT ASN1_UTCTIME *ASN1_UTCTIME_adj(ASN1_UTCTIME *s, time_t t, int offset_day, long offset_sec);
OPENSSL_EXPORT int ASN1_UTCTIME_set_string(ASN1_UTCTIME *s, const char *str);
OPENSSL_EXPORT int ASN1_UTCTIME_cmp_time_t(const ASN1_UTCTIME *s, time_t t);
#if 0
time_t ASN1_UTCTIME_get(const ASN1_UTCTIME *s);
#endif

OPENSSL_EXPORT int ASN1_GENERALIZEDTIME_check(const ASN1_GENERALIZEDTIME *a);
OPENSSL_EXPORT ASN1_GENERALIZEDTIME *ASN1_GENERALIZEDTIME_set(ASN1_GENERALIZEDTIME *s,time_t t);
OPENSSL_EXPORT ASN1_GENERALIZEDTIME *ASN1_GENERALIZEDTIME_adj(ASN1_GENERALIZEDTIME *s, time_t t, int offset_day, long offset_sec);
OPENSSL_EXPORT int ASN1_GENERALIZEDTIME_set_string(ASN1_GENERALIZEDTIME *s, const char *str);
OPENSSL_EXPORT int ASN1_TIME_diff(int *pday, int *psec, const ASN1_TIME *from, const ASN1_TIME *to);

DECLARE_ASN1_FUNCTIONS(ASN1_OCTET_STRING)
OPENSSL_EXPORT ASN1_OCTET_STRING *	ASN1_OCTET_STRING_dup(const ASN1_OCTET_STRING *a);
OPENSSL_EXPORT int 	ASN1_OCTET_STRING_cmp(const ASN1_OCTET_STRING *a, const ASN1_OCTET_STRING *b);
OPENSSL_EXPORT int 	ASN1_OCTET_STRING_set(ASN1_OCTET_STRING *str, const unsigned char *data, int len);

DECLARE_ASN1_FUNCTIONS(ASN1_VISIBLESTRING)
DECLARE_ASN1_FUNCTIONS(ASN1_UNIVERSALSTRING)
DECLARE_ASN1_FUNCTIONS(ASN1_UTF8STRING)
DECLARE_ASN1_FUNCTIONS(ASN1_NULL)
DECLARE_ASN1_FUNCTIONS(ASN1_BMPSTRING)

DECLARE_ASN1_FUNCTIONS_name(ASN1_STRING, ASN1_PRINTABLE)

DECLARE_ASN1_FUNCTIONS_name(ASN1_STRING, DIRECTORYSTRING)
DECLARE_ASN1_FUNCTIONS_name(ASN1_STRING, DISPLAYTEXT)
DECLARE_ASN1_FUNCTIONS(ASN1_PRINTABLESTRING)
DECLARE_ASN1_FUNCTIONS(ASN1_T61STRING)
DECLARE_ASN1_FUNCTIONS(ASN1_IA5STRING)
DECLARE_ASN1_FUNCTIONS(ASN1_GENERALSTRING)
DECLARE_ASN1_FUNCTIONS(ASN1_UTCTIME)
DECLARE_ASN1_FUNCTIONS(ASN1_GENERALIZEDTIME)
DECLARE_ASN1_FUNCTIONS(ASN1_TIME)

DECLARE_ASN1_ITEM(ASN1_OCTET_STRING_NDEF)

OPENSSL_EXPORT ASN1_TIME *ASN1_TIME_set(ASN1_TIME *s,time_t t);
OPENSSL_EXPORT ASN1_TIME *ASN1_TIME_adj(ASN1_TIME *s,time_t t, int offset_day, long offset_sec);
OPENSSL_EXPORT int ASN1_TIME_check(ASN1_TIME *t);
OPENSSL_EXPORT ASN1_GENERALIZEDTIME *ASN1_TIME_to_generalizedtime(ASN1_TIME *t, ASN1_GENERALIZEDTIME **out);
OPENSSL_EXPORT int ASN1_TIME_set_string(ASN1_TIME *s, const char *str);

OPENSSL_EXPORT int i2a_ASN1_INTEGER(BIO *bp, ASN1_INTEGER *a);
OPENSSL_EXPORT int i2a_ASN1_ENUMERATED(BIO *bp, ASN1_ENUMERATED *a);
OPENSSL_EXPORT int i2a_ASN1_OBJECT(BIO *bp,ASN1_OBJECT *a);
OPENSSL_EXPORT int i2a_ASN1_STRING(BIO *bp, ASN1_STRING *a, int type);
OPENSSL_EXPORT int i2t_ASN1_OBJECT(char *buf,int buf_len,ASN1_OBJECT *a);

OPENSSL_EXPORT ASN1_OBJECT *ASN1_OBJECT_create(int nid, unsigned char *data,int len, const char *sn, const char *ln);

OPENSSL_EXPORT int ASN1_INTEGER_set(ASN1_INTEGER *a, long v);
OPENSSL_EXPORT int ASN1_INTEGER_set_uint64(ASN1_INTEGER *out, uint64_t v);
OPENSSL_EXPORT long ASN1_INTEGER_get(const ASN1_INTEGER *a);
OPENSSL_EXPORT ASN1_INTEGER *BN_to_ASN1_INTEGER(const BIGNUM *bn, ASN1_INTEGER *ai);
OPENSSL_EXPORT BIGNUM *ASN1_INTEGER_to_BN(const ASN1_INTEGER *ai,BIGNUM *bn);

OPENSSL_EXPORT int ASN1_ENUMERATED_set(ASN1_ENUMERATED *a, long v);
OPENSSL_EXPORT long ASN1_ENUMERATED_get(ASN1_ENUMERATED *a);
OPENSSL_EXPORT ASN1_ENUMERATED *BN_to_ASN1_ENUMERATED(BIGNUM *bn, ASN1_ENUMERATED *ai);
OPENSSL_EXPORT BIGNUM *ASN1_ENUMERATED_to_BN(ASN1_ENUMERATED *ai,BIGNUM *bn);

/* General */
/* given a string, return the correct type, max is the maximum length */
OPENSSL_EXPORT int ASN1_PRINTABLE_type(const unsigned char *s, int max);

OPENSSL_EXPORT unsigned long ASN1_tag2bit(int tag);

/* SPECIALS */
OPENSSL_EXPORT int ASN1_get_object(const unsigned char **pp, long *plength, int *ptag, int *pclass, long omax);
OPENSSL_EXPORT void ASN1_put_object(unsigned char **pp, int constructed, int length, int tag, int xclass);
OPENSSL_EXPORT int ASN1_put_eoc(unsigned char **pp);
OPENSSL_EXPORT int ASN1_object_size(int constructed, int length, int tag);

/* Used to implement other functions */
OPENSSL_EXPORT void *ASN1_dup(i2d_of_void *i2d, d2i_of_void *d2i, void *x);

#define ASN1_dup_of(type,i2d,d2i,x) \
    ((type*)ASN1_dup(CHECKED_I2D_OF(type, i2d), \
		     CHECKED_D2I_OF(type, d2i), \
		     CHECKED_PTR_OF(type, x)))

#define ASN1_dup_of_const(type,i2d,d2i,x) \
    ((type*)ASN1_dup(CHECKED_I2D_OF(const type, i2d), \
		     CHECKED_D2I_OF(type, d2i), \
		     CHECKED_PTR_OF(const type, x)))

OPENSSL_EXPORT void *ASN1_item_dup(const ASN1_ITEM *it, void *x);

/* ASN1 alloc/free macros for when a type is only used internally */

#define M_ASN1_new_of(type) (type *)ASN1_item_new(ASN1_ITEM_rptr(type))
#define M_ASN1_free_of(x, type) \
		ASN1_item_free(CHECKED_PTR_OF(type, x), ASN1_ITEM_rptr(type))

#ifndef OPENSSL_NO_FP_API
OPENSSL_EXPORT void *ASN1_d2i_fp(void *(*xnew)(void), d2i_of_void *d2i, FILE *in, void **x);

#define ASN1_d2i_fp_of(type,xnew,d2i,in,x) \
    ((type*)ASN1_d2i_fp(CHECKED_NEW_OF(type, xnew), \
			CHECKED_D2I_OF(type, d2i), \
			in, \
			CHECKED_PPTR_OF(type, x)))

OPENSSL_EXPORT void *ASN1_item_d2i_fp(const ASN1_ITEM *it, FILE *in, void *x);
OPENSSL_EXPORT int ASN1_i2d_fp(i2d_of_void *i2d,FILE *out,void *x);

#define ASN1_i2d_fp_of(type,i2d,out,x) \
    (ASN1_i2d_fp(CHECKED_I2D_OF(type, i2d), \
		 out, \
		 CHECKED_PTR_OF(type, x)))

#define ASN1_i2d_fp_of_const(type,i2d,out,x) \
    (ASN1_i2d_fp(CHECKED_I2D_OF(const type, i2d), \
		 out, \
		 CHECKED_PTR_OF(const type, x)))

OPENSSL_EXPORT int ASN1_item_i2d_fp(const ASN1_ITEM *it, FILE *out, void *x);
OPENSSL_EXPORT int ASN1_STRING_print_ex_fp(FILE *fp, ASN1_STRING *str, unsigned long flags);
#endif

OPENSSL_EXPORT int ASN1_STRING_to_UTF8(unsigned char **out, ASN1_STRING *in);

OPENSSL_EXPORT void *ASN1_d2i_bio(void *(*xnew)(void), d2i_of_void *d2i, BIO *in, void **x);

#define ASN1_d2i_bio_of(type,xnew,d2i,in,x) \
    ((type*)ASN1_d2i_bio( CHECKED_NEW_OF(type, xnew), \
			  CHECKED_D2I_OF(type, d2i), \
			  in, \
			  CHECKED_PPTR_OF(type, x)))

OPENSSL_EXPORT void *ASN1_item_d2i_bio(const ASN1_ITEM *it, BIO *in, void *x);
OPENSSL_EXPORT int ASN1_i2d_bio(i2d_of_void *i2d,BIO *out, void *x);

#define ASN1_i2d_bio_of(type,i2d,out,x) \
    (ASN1_i2d_bio(CHECKED_I2D_OF(type, i2d), \
		  out, \
		  CHECKED_PTR_OF(type, x)))

#define ASN1_i2d_bio_of_const(type,i2d,out,x) \
    (ASN1_i2d_bio(CHECKED_I2D_OF(const type, i2d), \
		  out, \
		  CHECKED_PTR_OF(const type, x)))

OPENSSL_EXPORT int ASN1_item_i2d_bio(const ASN1_ITEM *it, BIO *out, void *x);
OPENSSL_EXPORT int ASN1_UTCTIME_print(BIO *fp, const ASN1_UTCTIME *a);
OPENSSL_EXPORT int ASN1_GENERALIZEDTIME_print(BIO *fp, const ASN1_GENERALIZEDTIME *a);
OPENSSL_EXPORT int ASN1_TIME_print(BIO *fp, const ASN1_TIME *a);
OPENSSL_EXPORT int ASN1_STRING_print(BIO *bp, const ASN1_STRING *v);
OPENSSL_EXPORT int ASN1_STRING_print_ex(BIO *out, ASN1_STRING *str, unsigned long flags);
OPENSSL_EXPORT const char *ASN1_tag2str(int tag);

/* Used to load and write netscape format cert */

OPENSSL_EXPORT void *ASN1_item_unpack(ASN1_STRING *oct, const ASN1_ITEM *it);

OPENSSL_EXPORT ASN1_STRING *ASN1_item_pack(void *obj, const ASN1_ITEM *it, ASN1_OCTET_STRING **oct);

OPENSSL_EXPORT void ASN1_STRING_set_default_mask(unsigned long mask);
OPENSSL_EXPORT int ASN1_STRING_set_default_mask_asc(const char *p);
OPENSSL_EXPORT unsigned long ASN1_STRING_get_default_mask(void);
OPENSSL_EXPORT int ASN1_mbstring_copy(ASN1_STRING **out, const unsigned char *in, int len, int inform, unsigned long mask);
OPENSSL_EXPORT int ASN1_mbstring_ncopy(ASN1_STRING **out, const unsigned char *in, int len, int inform, unsigned long mask, long minsize, long maxsize);

OPENSSL_EXPORT ASN1_STRING *ASN1_STRING_set_by_NID(ASN1_STRING **out, const unsigned char *in, int inlen, int inform, int nid);
OPENSSL_EXPORT ASN1_STRING_TABLE *ASN1_STRING_TABLE_get(int nid);
OPENSSL_EXPORT int ASN1_STRING_TABLE_add(int, long, long, unsigned long, unsigned long);
OPENSSL_EXPORT void ASN1_STRING_TABLE_cleanup(void);

/* ASN1 template functions */

/* Old API compatible functions */
OPENSSL_EXPORT ASN1_VALUE *ASN1_item_new(const ASN1_ITEM *it);
OPENSSL_EXPORT void ASN1_item_free(ASN1_VALUE *val, const ASN1_ITEM *it);
OPENSSL_EXPORT ASN1_VALUE * ASN1_item_d2i(ASN1_VALUE **val, const unsigned char **in, long len, const ASN1_ITEM *it);
OPENSSL_EXPORT int ASN1_item_i2d(ASN1_VALUE *val, unsigned char **out, const ASN1_ITEM *it);
OPENSSL_EXPORT int ASN1_item_ndef_i2d(ASN1_VALUE *val, unsigned char **out, const ASN1_ITEM *it);

OPENSSL_EXPORT ASN1_TYPE *ASN1_generate_nconf(char *str, CONF *nconf);
OPENSSL_EXPORT ASN1_TYPE *ASN1_generate_v3(char *str, X509V3_CTX *cnf);


#ifdef  __cplusplus
}

extern "C++" {

namespace bssl {

BORINGSSL_MAKE_DELETER(ASN1_OBJECT, ASN1_OBJECT_free)
BORINGSSL_MAKE_DELETER(ASN1_STRING, ASN1_STRING_free)
BORINGSSL_MAKE_DELETER(ASN1_TYPE, ASN1_TYPE_free)

}  // namespace bssl

}  /* extern C++ */

#endif

#define ASN1_R_ASN1_LENGTH_MISMATCH 100
#define ASN1_R_AUX_ERROR 101
#define ASN1_R_BAD_GET_ASN1_OBJECT_CALL 102
#define ASN1_R_BAD_OBJECT_HEADER 103
#define ASN1_R_BMPSTRING_IS_WRONG_LENGTH 104
#define ASN1_R_BN_LIB 105
#define ASN1_R_BOOLEAN_IS_WRONG_LENGTH 106
#define ASN1_R_BUFFER_TOO_SMALL 107
#define ASN1_R_CONTEXT_NOT_INITIALISED 108
#define ASN1_R_DECODE_ERROR 109
#define ASN1_R_DEPTH_EXCEEDED 110
#define ASN1_R_DIGEST_AND_KEY_TYPE_NOT_SUPPORTED 111
#define ASN1_R_ENCODE_ERROR 112
#define ASN1_R_ERROR_GETTING_TIME 113
#define ASN1_R_EXPECTING_AN_ASN1_SEQUENCE 114
#define ASN1_R_EXPECTING_AN_INTEGER 115
#define ASN1_R_EXPECTING_AN_OBJECT 116
#define ASN1_R_EXPECTING_A_BOOLEAN 117
#define ASN1_R_EXPECTING_A_TIME 118
#define ASN1_R_EXPLICIT_LENGTH_MISMATCH 119
#define ASN1_R_EXPLICIT_TAG_NOT_CONSTRUCTED 120
#define ASN1_R_FIELD_MISSING 121
#define ASN1_R_FIRST_NUM_TOO_LARGE 122
#define ASN1_R_HEADER_TOO_LONG 123
#define ASN1_R_ILLEGAL_BITSTRING_FORMAT 124
#define ASN1_R_ILLEGAL_BOOLEAN 125
#define ASN1_R_ILLEGAL_CHARACTERS 126
#define ASN1_R_ILLEGAL_FORMAT 127
#define ASN1_R_ILLEGAL_HEX 128
#define ASN1_R_ILLEGAL_IMPLICIT_TAG 129
#define ASN1_R_ILLEGAL_INTEGER 130
#define ASN1_R_ILLEGAL_NESTED_TAGGING 131
#define ASN1_R_ILLEGAL_NULL 132
#define ASN1_R_ILLEGAL_NULL_VALUE 133
#define ASN1_R_ILLEGAL_OBJECT 134
#define ASN1_R_ILLEGAL_OPTIONAL_ANY 135
#define ASN1_R_ILLEGAL_OPTIONS_ON_ITEM_TEMPLATE 136
#define ASN1_R_ILLEGAL_TAGGED_ANY 137
#define ASN1_R_ILLEGAL_TIME_VALUE 138
#define ASN1_R_INTEGER_NOT_ASCII_FORMAT 139
#define ASN1_R_INTEGER_TOO_LARGE_FOR_LONG 140
#define ASN1_R_INVALID_BIT_STRING_BITS_LEFT 141
#define ASN1_R_INVALID_BMPSTRING 142
#define ASN1_R_INVALID_DIGIT 143
#define ASN1_R_INVALID_MODIFIER 144
#define ASN1_R_INVALID_NUMBER 145
#define ASN1_R_INVALID_OBJECT_ENCODING 146
#define ASN1_R_INVALID_SEPARATOR 147
#define ASN1_R_INVALID_TIME_FORMAT 148
#define ASN1_R_INVALID_UNIVERSALSTRING 149
#define ASN1_R_INVALID_UTF8STRING 150
#define ASN1_R_LIST_ERROR 151
#define ASN1_R_MISSING_ASN1_EOS 152
#define ASN1_R_MISSING_EOC 153
#define ASN1_R_MISSING_SECOND_NUMBER 154
#define ASN1_R_MISSING_VALUE 155
#define ASN1_R_MSTRING_NOT_UNIVERSAL 156
#define ASN1_R_MSTRING_WRONG_TAG 157
#define ASN1_R_NESTED_ASN1_ERROR 158
#define ASN1_R_NESTED_ASN1_STRING 159
#define ASN1_R_NON_HEX_CHARACTERS 160
#define ASN1_R_NOT_ASCII_FORMAT 161
#define ASN1_R_NOT_ENOUGH_DATA 162
#define ASN1_R_NO_MATCHING_CHOICE_TYPE 163
#define ASN1_R_NULL_IS_WRONG_LENGTH 164
#define ASN1_R_OBJECT_NOT_ASCII_FORMAT 165
#define ASN1_R_ODD_NUMBER_OF_CHARS 166
#define ASN1_R_SECOND_NUMBER_TOO_LARGE 167
#define ASN1_R_SEQUENCE_LENGTH_MISMATCH 168
#define ASN1_R_SEQUENCE_NOT_CONSTRUCTED 169
#define ASN1_R_SEQUENCE_OR_SET_NEEDS_CONFIG 170
#define ASN1_R_SHORT_LINE 171
#define ASN1_R_STREAMING_NOT_SUPPORTED 172
#define ASN1_R_STRING_TOO_LONG 173
#define ASN1_R_STRING_TOO_SHORT 174
#define ASN1_R_TAG_VALUE_TOO_HIGH 175
#define ASN1_R_TIME_NOT_ASCII_FORMAT 176
#define ASN1_R_TOO_LONG 177
#define ASN1_R_TYPE_NOT_CONSTRUCTED 178
#define ASN1_R_TYPE_NOT_PRIMITIVE 179
#define ASN1_R_UNEXPECTED_EOC 180
#define ASN1_R_UNIVERSALSTRING_IS_WRONG_LENGTH 181
#define ASN1_R_UNKNOWN_FORMAT 182
#define ASN1_R_UNKNOWN_MESSAGE_DIGEST_ALGORITHM 183
#define ASN1_R_UNKNOWN_SIGNATURE_ALGORITHM 184
#define ASN1_R_UNKNOWN_TAG 185
#define ASN1_R_UNSUPPORTED_ANY_DEFINED_BY_TYPE 186
#define ASN1_R_UNSUPPORTED_PUBLIC_KEY_TYPE 187
#define ASN1_R_UNSUPPORTED_TYPE 188
#define ASN1_R_WRONG_PUBLIC_KEY_TYPE 189
#define ASN1_R_WRONG_TAG 190
#define ASN1_R_WRONG_TYPE 191
#define ASN1_R_NESTED_TOO_DEEP 192

#endif
