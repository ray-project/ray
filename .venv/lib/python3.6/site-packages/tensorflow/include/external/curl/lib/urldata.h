#ifndef HEADER_CURL_URLDATA_H
#define HEADER_CURL_URLDATA_H
/***************************************************************************
 *                                  _   _ ____  _
 *  Project                     ___| | | |  _ \| |
 *                             / __| | | | |_) | |
 *                            | (__| |_| |  _ <| |___
 *                             \___|\___/|_| \_\_____|
 *
 * Copyright (C) 1998 - 2018, Daniel Stenberg, <daniel@haxx.se>, et al.
 *
 * This software is licensed as described in the file COPYING, which
 * you should have received as part of this distribution. The terms
 * are also available at https://curl.haxx.se/docs/copyright.html.
 *
 * You may opt to use, copy, modify, merge, publish, distribute and/or sell
 * copies of the Software, and permit persons to whom the Software is
 * furnished to do so, under the terms of the COPYING file.
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY
 * KIND, either express or implied.
 *
 ***************************************************************************/

/* This file is for lib internal stuff */

#include "curl_setup.h"

#define PORT_FTP 21
#define PORT_FTPS 990
#define PORT_TELNET 23
#define PORT_HTTP 80
#define PORT_HTTPS 443
#define PORT_DICT 2628
#define PORT_LDAP 389
#define PORT_LDAPS 636
#define PORT_TFTP 69
#define PORT_SSH 22
#define PORT_IMAP 143
#define PORT_IMAPS 993
#define PORT_POP3 110
#define PORT_POP3S 995
#define PORT_SMB 445
#define PORT_SMBS 445
#define PORT_SMTP 25
#define PORT_SMTPS 465 /* sometimes called SSMTP */
#define PORT_RTSP 554
#define PORT_RTMP 1935
#define PORT_RTMPT PORT_HTTP
#define PORT_RTMPS PORT_HTTPS
#define PORT_GOPHER 70

#define DICT_MATCH "/MATCH:"
#define DICT_MATCH2 "/M:"
#define DICT_MATCH3 "/FIND:"
#define DICT_DEFINE "/DEFINE:"
#define DICT_DEFINE2 "/D:"
#define DICT_DEFINE3 "/LOOKUP:"

#define CURL_DEFAULT_USER "anonymous"
#define CURL_DEFAULT_PASSWORD "ftp@example.com"

/* Convenience defines for checking protocols or their SSL based version. Each
   protocol handler should only ever have a single CURLPROTO_ in its protocol
   field. */
#define PROTO_FAMILY_HTTP (CURLPROTO_HTTP|CURLPROTO_HTTPS)
#define PROTO_FAMILY_FTP  (CURLPROTO_FTP|CURLPROTO_FTPS)
#define PROTO_FAMILY_POP3 (CURLPROTO_POP3|CURLPROTO_POP3S)
#define PROTO_FAMILY_SMB  (CURLPROTO_SMB|CURLPROTO_SMBS)
#define PROTO_FAMILY_SMTP (CURLPROTO_SMTP|CURLPROTO_SMTPS)

#define DEFAULT_CONNCACHE_SIZE 5

/* length of longest IPv6 address string including the trailing null */
#define MAX_IPADR_LEN sizeof("ffff:ffff:ffff:ffff:ffff:ffff:255.255.255.255")

/* Default FTP/IMAP etc response timeout in milliseconds.
   Symbian OS panics when given a timeout much greater than 1/2 hour.
*/
#define RESP_TIMEOUT (1800*1000)

#include "cookie.h"
#include "formdata.h"

#ifdef HAVE_NETINET_IN_H
#include <netinet/in.h>
#endif
#ifdef HAVE_NETINET_IN6_H
#include <netinet/in6.h>
#endif

#include "timeval.h"

#include <curl/curl.h>

#include "http_chunks.h" /* for the structs and enum stuff */
#include "hostip.h"
#include "hash.h"
#include "splay.h"

/* return the count of bytes sent, or -1 on error */
typedef ssize_t (Curl_send)(struct connectdata *conn, /* connection data */
                            int sockindex,            /* socketindex */
                            const void *buf,          /* data to write */
                            size_t len,               /* max amount to write */
                            CURLcode *err);           /* error to return */

/* return the count of bytes read, or -1 on error */
typedef ssize_t (Curl_recv)(struct connectdata *conn, /* connection data */
                            int sockindex,            /* socketindex */
                            char *buf,                /* store data here */
                            size_t len,               /* max amount to read */
                            CURLcode *err);           /* error to return */

#include "mime.h"
#include "imap.h"
#include "pop3.h"
#include "smtp.h"
#include "ftp.h"
#include "file.h"
#include "ssh.h"
#include "http.h"
#include "rtsp.h"
#include "smb.h"
#include "wildcard.h"
#include "multihandle.h"

#ifdef HAVE_GSSAPI
# ifdef HAVE_GSSGNU
#  include <gss.h>
# elif defined HAVE_GSSMIT
#  include <gssapi/gssapi.h>
#  include <gssapi/gssapi_generic.h>
# else
#  include <gssapi.h>
# endif
#endif

#ifdef HAVE_LIBSSH2_H
#include <libssh2.h>
#include <libssh2_sftp.h>
#endif /* HAVE_LIBSSH2_H */

/* The upload buffer size, should not be smaller than CURL_MAX_WRITE_SIZE, as
   it needs to hold a full buffer as could be sent in a write callback */
#define UPLOAD_BUFSIZE CURL_MAX_WRITE_SIZE

/* The "master buffer" is for HTTP pipelining */
#define MASTERBUF_SIZE 16384

/* Initial size of the buffer to store headers in, it'll be enlarged in case
   of need. */
#define HEADERSIZE 256

#define CURLEASY_MAGIC_NUMBER 0xc0dedbadU
#define GOOD_EASY_HANDLE(x) \
  ((x) && ((x)->magic == CURLEASY_MAGIC_NUMBER))

/* Some convenience macros to get the larger/smaller value out of two given.
   We prefix with CURL to prevent name collisions. */
#define CURLMAX(x,y) ((x)>(y)?(x):(y))
#define CURLMIN(x,y) ((x)<(y)?(x):(y))

#ifdef HAVE_GSSAPI
/* Types needed for krb5-ftp connections */
struct krb5buffer {
  void *data;
  size_t size;
  size_t index;
  int eof_flag;
};

enum protection_level {
  PROT_NONE, /* first in list */
  PROT_CLEAR,
  PROT_SAFE,
  PROT_CONFIDENTIAL,
  PROT_PRIVATE,
  PROT_CMD,
  PROT_LAST /* last in list */
};
#endif

/* enum for the nonblocking SSL connection state machine */
typedef enum {
  ssl_connect_1,
  ssl_connect_2,
  ssl_connect_2_reading,
  ssl_connect_2_writing,
  ssl_connect_3,
  ssl_connect_done
} ssl_connect_state;

typedef enum {
  ssl_connection_none,
  ssl_connection_negotiating,
  ssl_connection_complete
} ssl_connection_state;

/* SSL backend-specific data; declared differently by each SSL backend */
struct ssl_backend_data;

/* struct for data related to each SSL connection */
struct ssl_connect_data {
  /* Use ssl encrypted communications TRUE/FALSE, not necessarily using it atm
     but at least asked to or meaning to use it. See 'state' for the exact
     current state of the connection. */
  bool use;
  ssl_connection_state state;
  ssl_connect_state connecting_state;
#if defined(USE_SSL)
  struct ssl_backend_data *backend;
#endif
};

struct ssl_primary_config {
  long version;          /* what version the client wants to use */
  long version_max;      /* max supported version the client wants to use*/
  bool verifypeer;       /* set TRUE if this is desired */
  bool verifyhost;       /* set TRUE if CN/SAN must match hostname */
  bool verifystatus;     /* set TRUE if certificate status must be checked */
  bool sessionid;        /* cache session IDs or not */
  char *CApath;          /* certificate dir (doesn't work on windows) */
  char *CAfile;          /* certificate to verify peer against */
  char *clientcert;
  char *random_file;     /* path to file containing "random" data */
  char *egdsocket;       /* path to file containing the EGD daemon socket */
  char *cipher_list;     /* list of ciphers to use */
};

struct ssl_config_data {
  struct ssl_primary_config primary;
  bool enable_beast; /* especially allow this flaw for interoperability's
                        sake*/
  bool no_revoke;    /* disable SSL certificate revocation checks */
  long certverifyresult; /* result from the certificate verification */
  char *CRLfile;   /* CRL to check certificate revocation */
  char *issuercert;/* optional issuer certificate filename */
  curl_ssl_ctx_callback fsslctx; /* function to initialize ssl ctx */
  void *fsslctxp;        /* parameter for call back */
  bool certinfo;         /* gather lots of certificate info */
  bool falsestart;

  char *cert; /* client certificate file name */
  char *cert_type; /* format for certificate (default: PEM)*/
  char *key; /* private key file name */
  char *key_type; /* format for private key (default: PEM) */
  char *key_passwd; /* plain text private key password */

#ifdef USE_TLS_SRP
  char *username; /* TLS username (for, e.g., SRP) */
  char *password; /* TLS password (for, e.g., SRP) */
  enum CURL_TLSAUTH authtype; /* TLS authentication type (default SRP) */
#endif
};

struct ssl_general_config {
  size_t max_ssl_sessions; /* SSL session id cache size */
};

/* information stored about one single SSL session */
struct curl_ssl_session {
  char *name;       /* host name for which this ID was used */
  char *conn_to_host; /* host name for the connection (may be NULL) */
  const char *scheme; /* protocol scheme used */
  void *sessionid;  /* as returned from the SSL layer */
  size_t idsize;    /* if known, otherwise 0 */
  long age;         /* just a number, the higher the more recent */
  int remote_port;  /* remote port */
  int conn_to_port; /* remote port for the connection (may be -1) */
  struct ssl_primary_config ssl_config; /* setup for this session */
};

#ifdef USE_WINDOWS_SSPI
#include "curl_sspi.h"
#endif

/* Struct used for Digest challenge-response authentication */
struct digestdata {
#if defined(USE_WINDOWS_SSPI)
  BYTE *input_token;
  size_t input_token_len;
  CtxtHandle *http_context;
  /* copy of user/passwd used to make the identity for http_context.
     either may be NULL. */
  char *user;
  char *passwd;
#else
  char *nonce;
  char *cnonce;
  char *realm;
  int algo;
  bool stale; /* set true for re-negotiation */
  char *opaque;
  char *qop;
  char *algorithm;
  int nc; /* nounce count */
  bool userhash;
#endif
};

typedef enum {
  NTLMSTATE_NONE,
  NTLMSTATE_TYPE1,
  NTLMSTATE_TYPE2,
  NTLMSTATE_TYPE3,
  NTLMSTATE_LAST
} curlntlm;

#if defined(CURL_DOES_CONVERSIONS) && defined(HAVE_ICONV)
#include <iconv.h>
#endif

/* Struct used for GSSAPI (Kerberos V5) authentication */
#if defined(USE_KERBEROS5)
struct kerberos5data {
#if defined(USE_WINDOWS_SSPI)
  CredHandle *credentials;
  CtxtHandle *context;
  TCHAR *spn;
  SEC_WINNT_AUTH_IDENTITY identity;
  SEC_WINNT_AUTH_IDENTITY *p_identity;
  size_t token_max;
  BYTE *output_token;
#else
  gss_ctx_id_t context;
  gss_name_t spn;
#endif
};
#endif

/* Struct used for NTLM challenge-response authentication */
#if defined(USE_NTLM)
struct ntlmdata {
  curlntlm state;
#ifdef USE_WINDOWS_SSPI
  CredHandle *credentials;
  CtxtHandle *context;
  SEC_WINNT_AUTH_IDENTITY identity;
  SEC_WINNT_AUTH_IDENTITY *p_identity;
  size_t token_max;
  BYTE *output_token;
  BYTE *input_token;
  size_t input_token_len;
  TCHAR *spn;
#else
  unsigned int flags;
  unsigned char nonce[8];
  void *target_info; /* TargetInfo received in the ntlm type-2 message */
  unsigned int target_info_len;
#endif
};
#endif

#ifdef USE_SPNEGO
struct negotiatedata {
  /* When doing Negotiate (SPNEGO) auth, we first need to send a token
     and then validate the received one. */
  enum { GSS_AUTHNONE, GSS_AUTHRECV, GSS_AUTHSENT } state;
#ifdef HAVE_GSSAPI
  OM_uint32 status;
  gss_ctx_id_t context;
  gss_name_t spn;
  gss_buffer_desc output_token;
#else
#ifdef USE_WINDOWS_SSPI
  DWORD status;
  CredHandle *credentials;
  CtxtHandle *context;
  SEC_WINNT_AUTH_IDENTITY identity;
  SEC_WINNT_AUTH_IDENTITY *p_identity;
  TCHAR *spn;
  size_t token_max;
  BYTE *output_token;
  size_t output_token_length;
#endif
#endif
};
#endif


/*
 * Boolean values that concerns this connection.
 */
struct ConnectBits {
  /* always modify bits.close with the connclose() and connkeep() macros! */
  bool close; /* if set, we close the connection after this request */
  bool reuse; /* if set, this is a re-used connection */
  bool conn_to_host; /* if set, this connection has a "connect to host"
                        that overrides the host in the URL */
  bool conn_to_port; /* if set, this connection has a "connect to port"
                        that overrides the port in the URL (remote port) */
  bool proxy; /* if set, this transfer is done through a proxy - any type */
  bool httpproxy;    /* if set, this transfer is done through a http proxy */
  bool socksproxy;   /* if set, this transfer is done through a socks proxy */
  bool user_passwd;    /* do we use user+password for this connection? */
  bool proxy_user_passwd; /* user+password for the proxy? */
  bool ipv6_ip; /* we communicate with a remote site specified with pure IPv6
                   IP address */
  bool ipv6;    /* we communicate with a site using an IPv6 address */

  bool do_more; /* this is set TRUE if the ->curl_do_more() function is
                   supposed to be called, after ->curl_do() */
  bool tcpconnect[2]; /* the TCP layer (or similar) is connected, this is set
                         the first time on the first connect function call */
  bool protoconnstart;/* the protocol layer has STARTED its operation after
                         the TCP layer connect */

  bool retry;         /* this connection is about to get closed and then
                         re-attempted at another connection. */
  bool tunnel_proxy;  /* if CONNECT is used to "tunnel" through the proxy.
                         This is implicit when SSL-protocols are used through
                         proxies, but can also be enabled explicitly by
                         apps */
  bool authneg;       /* TRUE when the auth phase has started, which means
                         that we are creating a request with an auth header,
                         but it is not the final request in the auth
                         negotiation. */
  bool rewindaftersend;/* TRUE when the sending couldn't be stopped even
                          though it will be discarded. When the whole send
                          operation is done, we must call the data rewind
                          callback. */
  bool ftp_use_epsv;  /* As set with CURLOPT_FTP_USE_EPSV, but if we find out
                         EPSV doesn't work we disable it for the forthcoming
                         requests */

  bool ftp_use_eprt;  /* As set with CURLOPT_FTP_USE_EPRT, but if we find out
                         EPRT doesn't work we disable it for the forthcoming
                         requests */
  bool ftp_use_data_ssl; /* Enabled SSL for the data connection */
  bool netrc;         /* name+password provided by netrc */
  bool userpwd_in_url; /* name+password found in url */
  bool stream_was_rewound; /* Indicates that the stream was rewound after a
                              request read past the end of its response byte
                              boundary */
  bool proxy_connect_closed; /* set true if a proxy disconnected the
                                connection in a CONNECT request with auth, so
                                that libcurl should reconnect and continue. */
  bool bound; /* set true if bind() has already been done on this socket/
                 connection */
  bool type_set;  /* type= was used in the URL */
  bool multiplex; /* connection is multiplexed */

  bool tcp_fastopen; /* use TCP Fast Open */
  bool tls_enable_npn;  /* TLS NPN extension? */
  bool tls_enable_alpn; /* TLS ALPN extension? */
  bool proxy_ssl_connected[2]; /* TRUE when SSL initialization for HTTPS proxy
                                  is complete */
  bool socksproxy_connecting; /* connecting through a socks proxy */
};

struct hostname {
  char *rawalloc; /* allocated "raw" version of the name */
  char *encalloc; /* allocated IDN-encoded version of the name */
  char *name;     /* name to use internally, might be encoded, might be raw */
  const char *dispname; /* name to display, as 'name' might be encoded */
};

/*
 * Flags on the keepon member of the Curl_transfer_keeper
 */

#define KEEP_NONE  0
#define KEEP_RECV  (1<<0)     /* there is or may be data to read */
#define KEEP_SEND (1<<1)     /* there is or may be data to write */
#define KEEP_RECV_HOLD (1<<2) /* when set, no reading should be done but there
                                 might still be data to read */
#define KEEP_SEND_HOLD (1<<3) /* when set, no writing should be done but there
                                  might still be data to write */
#define KEEP_RECV_PAUSE (1<<4) /* reading is paused */
#define KEEP_SEND_PAUSE (1<<5) /* writing is paused */

#define KEEP_RECVBITS (KEEP_RECV | KEEP_RECV_HOLD | KEEP_RECV_PAUSE)
#define KEEP_SENDBITS (KEEP_SEND | KEEP_SEND_HOLD | KEEP_SEND_PAUSE)


#ifdef CURLRES_ASYNCH
struct Curl_async {
  char *hostname;
  int port;
  struct Curl_dns_entry *dns;
  bool done;  /* set TRUE when the lookup is complete */
  int status; /* if done is TRUE, this is the status from the callback */
  void *os_specific;  /* 'struct thread_data' for Windows */
};
#endif

#define FIRSTSOCKET     0
#define SECONDARYSOCKET 1

/* These function pointer types are here only to allow easier typecasting
   within the source when we need to cast between data pointers (such as NULL)
   and function pointers. */
typedef CURLcode (*Curl_do_more_func)(struct connectdata *, int *);
typedef CURLcode (*Curl_done_func)(struct connectdata *, CURLcode, bool);

enum expect100 {
  EXP100_SEND_DATA,           /* enough waiting, just send the body now */
  EXP100_AWAITING_CONTINUE,   /* waiting for the 100 Continue header */
  EXP100_SENDING_REQUEST,     /* still sending the request but will wait for
                                 the 100 header once done with the request */
  EXP100_FAILED               /* used on 417 Expectation Failed */
};

enum upgrade101 {
  UPGR101_INIT,               /* default state */
  UPGR101_REQUESTED,          /* upgrade requested */
  UPGR101_RECEIVED,           /* response received */
  UPGR101_WORKING             /* talking upgraded protocol */
};

/*
 * Request specific data in the easy handle (Curl_easy).  Previously,
 * these members were on the connectdata struct but since a conn struct may
 * now be shared between different Curl_easys, we store connection-specific
 * data here. This struct only keeps stuff that's interesting for *this*
 * request, as it will be cleared between multiple ones
 */
struct SingleRequest {
  curl_off_t size;        /* -1 if unknown at this point */
  curl_off_t *bytecountp; /* return number of bytes read or NULL */

  curl_off_t maxdownload; /* in bytes, the maximum amount of data to fetch,
                             -1 means unlimited */
  curl_off_t *writebytecountp; /* return number of bytes written or NULL */

  curl_off_t bytecount;         /* total number of bytes read */
  curl_off_t writebytecount;    /* number of bytes written */

  long headerbytecount;         /* only count received headers */
  long deductheadercount; /* this amount of bytes doesn't count when we check
                             if anything has been transferred at the end of a
                             connection. We use this counter to make only a
                             100 reply (without a following second response
                             code) result in a CURLE_GOT_NOTHING error code */

  struct curltime start;         /* transfer started at this time */
  struct curltime now;           /* current time */
  bool header;                  /* incoming data has HTTP header */
  enum {
    HEADER_NORMAL,              /* no bad header at all */
    HEADER_PARTHEADER,          /* part of the chunk is a bad header, the rest
                                   is normal data */
    HEADER_ALLBAD               /* all was believed to be header */
  } badheader;                  /* the header was deemed bad and will be
                                   written as body */
  int headerline;               /* counts header lines to better track the
                                   first one */
  char *hbufp;                  /* points at *end* of header line */
  size_t hbuflen;
  char *str;                    /* within buf */
  char *str_start;              /* within buf */
  char *end_ptr;                /* within buf */
  char *p;                      /* within headerbuff */
  bool content_range;           /* set TRUE if Content-Range: was found */
  curl_off_t offset;            /* possible resume offset read from the
                                   Content-Range: header */
  int httpcode;                 /* error code from the 'HTTP/1.? XXX' or
                                   'RTSP/1.? XXX' line */
  struct curltime start100;      /* time stamp to wait for the 100 code from */
  enum expect100 exp100;        /* expect 100 continue state */
  enum upgrade101 upgr101;      /* 101 upgrade state */

  struct contenc_writer_s *writer_stack;  /* Content unencoding stack. */
                                          /* See sec 3.5, RFC2616. */
  time_t timeofdoc;
  long bodywrites;

  char *buf;
  curl_socket_t maxfd;

  int keepon;

  bool upload_done; /* set to TRUE when doing chunked transfer-encoding upload
                       and we're uploading the last chunk */

  bool ignorebody;  /* we read a response-body but we ignore it! */
  bool ignorecl;    /* This HTTP response has no body so we ignore the Content-
                       Length: header */

  char *location;   /* This points to an allocated version of the Location:
                       header data */
  char *newurl;     /* Set to the new URL to use when a redirect or a retry is
                       wanted */

  /* 'upload_present' is used to keep a byte counter of how much data there is
     still left in the buffer, aimed for upload. */
  ssize_t upload_present;

   /* 'upload_fromhere' is used as a read-pointer when we uploaded parts of a
      buffer, so the next read should read from where this pointer points to,
      and the 'upload_present' contains the number of bytes available at this
      position */
  char *upload_fromhere;

  bool chunk; /* if set, this is a chunked transfer-encoding */
  bool upload_chunky; /* set TRUE if we are doing chunked transfer-encoding
                         on upload */
  bool getheader;     /* TRUE if header parsing is wanted */

  bool forbidchunk;   /* used only to explicitly forbid chunk-upload for
                         specific upload buffers. See readmoredata() in
                         http.c for details. */

  void *protop;       /* Allocated protocol-specific data. Each protocol
                         handler makes sure this points to data it needs. */
};

/*
 * Specific protocol handler.
 */

struct Curl_handler {
  const char *scheme;        /* URL scheme name. */

  /* Complement to setup_connection_internals(). */
  CURLcode (*setup_connection)(struct connectdata *);

  /* These two functions MUST be set to be protocol dependent */
  CURLcode (*do_it)(struct connectdata *, bool *done);
  Curl_done_func done;

  /* If the curl_do() function is better made in two halves, this
   * curl_do_more() function will be called afterwards, if set. For example
   * for doing the FTP stuff after the PASV/PORT command.
   */
  Curl_do_more_func do_more;

  /* This function *MAY* be set to a protocol-dependent function that is run
   * after the connect() and everything is done, as a step in the connection.
   * The 'done' pointer points to a bool that should be set to TRUE if the
   * function completes before return. If it doesn't complete, the caller
   * should call the curl_connecting() function until it is.
   */
  CURLcode (*connect_it)(struct connectdata *, bool *done);

  /* See above. Currently only used for FTP. */
  CURLcode (*connecting)(struct connectdata *, bool *done);
  CURLcode (*doing)(struct connectdata *, bool *done);

  /* Called from the multi interface during the PROTOCONNECT phase, and it
     should then return a proper fd set */
  int (*proto_getsock)(struct connectdata *conn,
                       curl_socket_t *socks,
                       int numsocks);

  /* Called from the multi interface during the DOING phase, and it should
     then return a proper fd set */
  int (*doing_getsock)(struct connectdata *conn,
                       curl_socket_t *socks,
                       int numsocks);

  /* Called from the multi interface during the DO_MORE phase, and it should
     then return a proper fd set */
  int (*domore_getsock)(struct connectdata *conn,
                        curl_socket_t *socks,
                        int numsocks);

  /* Called from the multi interface during the DO_DONE, PERFORM and
     WAITPERFORM phases, and it should then return a proper fd set. Not setting
     this will make libcurl use the generic default one. */
  int (*perform_getsock)(const struct connectdata *conn,
                         curl_socket_t *socks,
                         int numsocks);

  /* This function *MAY* be set to a protocol-dependent function that is run
   * by the curl_disconnect(), as a step in the disconnection.  If the handler
   * is called because the connection has been considered dead, dead_connection
   * is set to TRUE.
   */
  CURLcode (*disconnect)(struct connectdata *, bool dead_connection);

  /* If used, this function gets called from transfer.c:readwrite_data() to
     allow the protocol to do extra reads/writes */
  CURLcode (*readwrite)(struct Curl_easy *data, struct connectdata *conn,
                        ssize_t *nread, bool *readmore);

  /* This function can perform various checks on the connection. See
     CONNCHECK_* for more information about the checks that can be performed,
     and CONNRESULT_* for the results that can be returned. */
  unsigned int (*connection_check)(struct connectdata *conn,
                                   unsigned int checks_to_perform);

  long defport;           /* Default port. */
  unsigned int protocol;  /* See CURLPROTO_* - this needs to be the single
                             specific protocol bit */
  unsigned int flags;     /* Extra particular characteristics, see PROTOPT_* */
};

#define PROTOPT_NONE 0             /* nothing extra */
#define PROTOPT_SSL (1<<0)         /* uses SSL */
#define PROTOPT_DUAL (1<<1)        /* this protocol uses two connections */
#define PROTOPT_CLOSEACTION (1<<2) /* need action before socket close */
/* some protocols will have to call the underlying functions without regard to
   what exact state the socket signals. IE even if the socket says "readable",
   the send function might need to be called while uploading, or vice versa.
*/
#define PROTOPT_DIRLOCK (1<<3)
#define PROTOPT_NONETWORK (1<<4)   /* protocol doesn't use the network! */
#define PROTOPT_NEEDSPWD (1<<5)    /* needs a password, and if none is set it
                                      gets a default */
#define PROTOPT_NOURLQUERY (1<<6)   /* protocol can't handle
                                        url query strings (?foo=bar) ! */
#define PROTOPT_CREDSPERREQUEST (1<<7) /* requires login credentials per
                                          request instead of per connection */
#define PROTOPT_ALPN_NPN (1<<8) /* set ALPN and/or NPN for this */
#define PROTOPT_STREAM (1<<9) /* a protocol with individual logical streams */
#define PROTOPT_URLOPTIONS (1<<10) /* allow options part in the userinfo field
                                      of the URL */
#define PROTOPT_PROXY_AS_HTTP (1<<11) /* allow this non-HTTP scheme over a
                                         HTTP proxy as HTTP proxies may know
                                         this protocol and act as a gateway */
#define PROTOPT_WILDCARD (1<<12) /* protocol supports wildcard matching */

#define CONNCHECK_NONE 0                 /* No checks */
#define CONNCHECK_ISDEAD (1<<0)          /* Check if the connection is dead. */

#define CONNRESULT_NONE 0                /* No extra information. */
#define CONNRESULT_DEAD (1<<0)           /* The connection is dead. */

#ifdef USE_RECV_BEFORE_SEND_WORKAROUND
struct postponed_data {
  char *buffer;          /* Temporal store for received data during
                            sending, must be freed */
  size_t allocated_size; /* Size of temporal store */
  size_t recv_size;      /* Size of received data during sending */
  size_t recv_processed; /* Size of processed part of postponed data */
#ifdef DEBUGBUILD
  curl_socket_t bindsock;/* Structure must be bound to specific socket,
                            used only for DEBUGASSERT */
#endif /* DEBUGBUILD */
};
#endif /* USE_RECV_BEFORE_SEND_WORKAROUND */

struct proxy_info {
  struct hostname host;
  long port;
  curl_proxytype proxytype; /* what kind of proxy that is in use */
  char *user;    /* proxy user name string, allocated */
  char *passwd;  /* proxy password string, allocated */
};

#define CONNECT_BUFFER_SIZE 16384

/* struct for HTTP CONNECT state data */
struct http_connect_state {
  char connect_buffer[CONNECT_BUFFER_SIZE];
  int perline; /* count bytes per line */
  int keepon;
  char *line_start;
  char *ptr; /* where to store more data */
  curl_off_t cl; /* size of content to read and ignore */
  bool chunked_encoding;
  enum {
    TUNNEL_INIT,    /* init/default/no tunnel state */
    TUNNEL_CONNECT, /* CONNECT has been sent off */
    TUNNEL_COMPLETE /* CONNECT response received completely */
  } tunnel_state;
  bool close_connection;
};

/*
 * The connectdata struct contains all fields and variables that should be
 * unique for an entire connection.
 */
struct connectdata {
  /* 'data' is the CURRENT Curl_easy using this connection -- take great
     caution that this might very well vary between different times this
     connection is used! */
  struct Curl_easy *data;

  struct curl_llist_element bundle_node; /* conncache */

  /* chunk is for HTTP chunked encoding, but is in the general connectdata
     struct only because we can do just about any protocol through a HTTP proxy
     and a HTTP proxy may in fact respond using chunked encoding */
  struct Curl_chunker chunk;

  curl_closesocket_callback fclosesocket; /* function closing the socket(s) */
  void *closesocket_client;

  bool inuse; /* This is a marker for the connection cache logic. If this is
                 TRUE this handle is being used by one or more easy handles
                 and can only used by any other easy handle without careful
                 consideration (== only for pipelining/multiplexing) and it
                 cannot be used by another multi handle! */

  /**** Fields set when inited and not modified again */
  long connection_id; /* Contains a unique number to make it easier to
                         track the connections in the log output */

  /* 'dns_entry' is the particular host we use. This points to an entry in the
     DNS cache and it will not get pruned while locked. It gets unlocked in
     Curl_done(). This entry will be NULL if the connection is re-used as then
     there is no name resolve done. */
  struct Curl_dns_entry *dns_entry;

  /* 'ip_addr' is the particular IP we connected to. It points to a struct
     within the DNS cache, so this pointer is only valid as long as the DNS
     cache entry remains locked. It gets unlocked in Curl_done() */
  Curl_addrinfo *ip_addr;
  Curl_addrinfo *tempaddr[2]; /* for happy eyeballs */

  /* 'ip_addr_str' is the ip_addr data as a human readable string.
     It remains available as long as the connection does, which is longer than
     the ip_addr itself. */
  char ip_addr_str[MAX_IPADR_LEN];

  unsigned int scope_id;  /* Scope id for IPv6 */

  int socktype;  /* SOCK_STREAM or SOCK_DGRAM */

  struct hostname host;
  char *secondaryhostname; /* secondary socket host name (ftp) */
  struct hostname conn_to_host; /* the host to connect to. valid only if
                                   bits.conn_to_host is set */

  struct proxy_info socks_proxy;
  struct proxy_info http_proxy;

  long port;       /* which port to use locally */
  int remote_port; /* the remote port, not the proxy port! */
  int conn_to_port; /* the remote port to connect to. valid only if
                       bits.conn_to_port is set */
  unsigned short secondary_port; /* secondary socket remote port to connect to
                                    (ftp) */

  /* 'primary_ip' and 'primary_port' get filled with peer's numerical
     ip address and port number whenever an outgoing connection is
     *attempted* from the primary socket to a remote address. When more
     than one address is tried for a connection these will hold data
     for the last attempt. When the connection is actually established
     these are updated with data which comes directly from the socket. */

  char primary_ip[MAX_IPADR_LEN];
  long primary_port;

  /* 'local_ip' and 'local_port' get filled with local's numerical
     ip address and port number whenever an outgoing connection is
     **established** from the primary socket to a remote address. */

  char local_ip[MAX_IPADR_LEN];
  long local_port;

  char *user;    /* user name string, allocated */
  char *passwd;  /* password string, allocated */
  char *options; /* options string, allocated */

  char *oauth_bearer; /* bearer token for OAuth 2.0, allocated */

  int httpversion;        /* the HTTP version*10 reported by the server */
  int rtspversion;        /* the RTSP version*10 reported by the server */

  struct curltime now;     /* "current" time */
  struct curltime created; /* creation time */
  curl_socket_t sock[2]; /* two sockets, the second is used for the data
                            transfer when doing FTP */
  curl_socket_t tempsock[2]; /* temporary sockets for happy eyeballs */
  bool sock_accepted[2]; /* TRUE if the socket on this index was created with
                            accept() */
  Curl_recv *recv[2];
  Curl_send *send[2];

#ifdef USE_RECV_BEFORE_SEND_WORKAROUND
  struct postponed_data postponed[2]; /* two buffers for two sockets */
#endif /* USE_RECV_BEFORE_SEND_WORKAROUND */
  struct ssl_connect_data ssl[2]; /* this is for ssl-stuff */
  struct ssl_connect_data proxy_ssl[2]; /* this is for proxy ssl-stuff */
#ifdef USE_SSL
  void *ssl_extra; /* separately allocated backend-specific data */
#endif
  struct ssl_primary_config ssl_config;
  struct ssl_primary_config proxy_ssl_config;
  bool tls_upgraded;

  struct ConnectBits bits;    /* various state-flags for this connection */

 /* connecttime: when connect() is called on the current IP address. Used to
    be able to track when to move on to try next IP - but only when the multi
    interface is used. */
  struct curltime connecttime;
  /* The two fields below get set in Curl_connecthost */
  int num_addr; /* number of addresses to try to connect to */
  time_t timeoutms_per_addr; /* how long time in milliseconds to spend on
                                trying to connect to each IP address */

  const struct Curl_handler *handler; /* Connection's protocol handler */
  const struct Curl_handler *given;   /* The protocol first given */

  long ip_version; /* copied from the Curl_easy at creation time */

  /**** curl_get() phase fields */

  curl_socket_t sockfd;   /* socket to read from or CURL_SOCKET_BAD */
  curl_socket_t writesockfd; /* socket to write to, it may very
                                well be the same we read from.
                                CURL_SOCKET_BAD disables */

  /** Dynamically allocated strings, MUST be freed before this **/
  /** struct is killed.                                      **/
  struct dynamically_allocated_data {
    char *proxyuserpwd;
    char *uagent;
    char *accept_encoding;
    char *userpwd;
    char *rangeline;
    char *ref;
    char *host;
    char *cookiehost;
    char *rtsp_transport;
    char *te; /* TE: request header */
  } allocptr;

#ifdef HAVE_GSSAPI
  int sec_complete; /* if Kerberos is enabled for this connection */
  enum protection_level command_prot;
  enum protection_level data_prot;
  enum protection_level request_data_prot;
  size_t buffer_size;
  struct krb5buffer in_buffer;
  void *app_data;
  const struct Curl_sec_client_mech *mech;
  struct sockaddr_in local_addr;
#endif

#if defined(USE_KERBEROS5)    /* Consider moving some of the above GSS-API */
  struct kerberos5data krb5;  /* variables into the structure definition, */
#endif                        /* however, some of them are ftp specific. */

  /* the two following *_inuse fields are only flags, not counters in any way.
     If TRUE it means the channel is in use, and if FALSE it means the channel
     is up for grabs by one. */

  bool readchannel_inuse;  /* whether the read channel is in use by an easy
                              handle */
  bool writechannel_inuse; /* whether the write channel is in use by an easy
                              handle */
  struct curl_llist send_pipe; /* List of handles waiting to send on this
                                  pipeline */
  struct curl_llist recv_pipe; /* List of handles waiting to read their
                                  responses on this pipeline */
  char *master_buffer; /* The master buffer allocated on-demand;
                          used for pipelining. */
  size_t read_pos; /* Current read position in the master buffer */
  size_t buf_len; /* Length of the buffer?? */


  curl_seek_callback seek_func; /* function that seeks the input */
  void *seek_client;            /* pointer to pass to the seek() above */

  /*************** Request - specific items ************/

#if defined(USE_NTLM)
  struct ntlmdata ntlm;     /* NTLM differs from other authentication schemes
                               because it authenticates connections, not
                               single requests! */
  struct ntlmdata proxyntlm; /* NTLM data for proxy */

#if defined(NTLM_WB_ENABLED)
  /* used for communication with Samba's winbind daemon helper ntlm_auth */
  curl_socket_t ntlm_auth_hlpr_socket;
  pid_t ntlm_auth_hlpr_pid;
  char *challenge_header;
  char *response_header;
#endif
#endif

  char syserr_buf [256]; /* buffer for Curl_strerror() */

#ifdef CURLRES_ASYNCH
  /* data used for the asynch name resolve callback */
  struct Curl_async async;
#endif

  /* These three are used for chunked-encoding trailer support */
  char *trailer; /* allocated buffer to store trailer in */
  int trlMax;    /* allocated buffer size */
  int trlPos;    /* index of where to store data */

  union {
    struct ftp_conn ftpc;
    struct http_conn httpc;
    struct ssh_conn sshc;
    struct tftp_state_data *tftpc;
    struct imap_conn imapc;
    struct pop3_conn pop3c;
    struct smtp_conn smtpc;
    struct rtsp_conn rtspc;
    struct smb_conn smbc;
    void *generic; /* RTMP and LDAP use this */
  } proto;

  int cselect_bits; /* bitmask of socket events */
  int waitfor;      /* current READ/WRITE bits to wait for */

#if defined(HAVE_GSSAPI) || defined(USE_WINDOWS_SSPI)
  int socks5_gssapi_enctype;
#endif

  /* When this connection is created, store the conditions for the local end
     bind. This is stored before the actual bind and before any connection is
     made and will serve the purpose of being used for comparison reasons so
     that subsequent bound-requested connections aren't accidentally re-using
     wrong connections. */
  char *localdev;
  unsigned short localport;
  int localportrange;
  struct http_connect_state *connect_state; /* for HTTP CONNECT */
  struct connectbundle *bundle; /* The bundle we are member of */
  int negnpn; /* APLN or NPN TLS negotiated protocol, CURL_HTTP_VERSION* */

#ifdef USE_UNIX_SOCKETS
  char *unix_domain_socket;
  bool abstract_unix_socket;
#endif
};

/* The end of connectdata. */

/*
 * Struct to keep statistical and informational data.
 * All variables in this struct must be initialized/reset in Curl_initinfo().
 */
struct PureInfo {
  int httpcode;  /* Recent HTTP, FTP, RTSP or SMTP response code */
  int httpproxycode; /* response code from proxy when received separate */
  int httpversion; /* the http version number X.Y = X*10+Y */
  time_t filetime; /* If requested, this is might get set. Set to -1 if the
                      time was unretrievable. */
  bool timecond;  /* set to TRUE if the time condition didn't match, which
                     thus made the document NOT get fetched */
  long header_size;  /* size of read header(s) in bytes */
  long request_size; /* the amount of bytes sent in the request(s) */
  unsigned long proxyauthavail; /* what proxy auth types were announced */
  unsigned long httpauthavail;  /* what host auth types were announced */
  long numconnects; /* how many new connection did libcurl created */
  char *contenttype; /* the content type of the object */
  char *wouldredirect; /* URL this would've been redirected to if asked to */

  /* PureInfo members 'conn_primary_ip', 'conn_primary_port', 'conn_local_ip'
     and, 'conn_local_port' are copied over from the connectdata struct in
     order to allow curl_easy_getinfo() to return this information even when
     the session handle is no longer associated with a connection, and also
     allow curl_easy_reset() to clear this information from the session handle
     without disturbing information which is still alive, and that might be
     reused, in the connection cache. */

  char conn_primary_ip[MAX_IPADR_LEN];
  long conn_primary_port;

  char conn_local_ip[MAX_IPADR_LEN];
  long conn_local_port;

  const char *conn_scheme;
  unsigned int conn_protocol;

  struct curl_certinfo certs; /* info about the certs, only populated in
                                 OpenSSL builds. Asked for with
                                 CURLOPT_CERTINFO / CURLINFO_CERTINFO */
};


struct Progress {
  time_t lastshow; /* time() of the last displayed progress meter or NULL to
                      force redraw at next call */
  curl_off_t size_dl; /* total expected size */
  curl_off_t size_ul; /* total expected size */
  curl_off_t downloaded; /* transferred so far */
  curl_off_t uploaded; /* transferred so far */

  curl_off_t current_speed; /* uses the currently fastest transfer */

  bool callback;  /* set when progress callback is used */
  int width; /* screen width at download start */
  int flags; /* see progress.h */

  time_t timespent;

  curl_off_t dlspeed;
  curl_off_t ulspeed;

  time_t t_nslookup;
  time_t t_connect;
  time_t t_appconnect;
  time_t t_pretransfer;
  time_t t_starttransfer;
  time_t t_redirect;

  struct curltime start;
  struct curltime t_startsingle;
  struct curltime t_startop;
  struct curltime t_acceptdata;

  bool is_t_startransfer_set;

  /* upload speed limit */
  struct curltime ul_limit_start;
  curl_off_t ul_limit_size;
  /* download speed limit */
  struct curltime dl_limit_start;
  curl_off_t dl_limit_size;

#define CURR_TIME (5 + 1) /* 6 entries for 5 seconds */

  curl_off_t speeder[ CURR_TIME ];
  struct curltime speeder_time[ CURR_TIME ];
  int speeder_c;
};

typedef enum {
  HTTPREQ_NONE, /* first in list */
  HTTPREQ_GET,
  HTTPREQ_POST,
  HTTPREQ_POST_FORM, /* we make a difference internally */
  HTTPREQ_POST_MIME, /* we make a difference internally */
  HTTPREQ_PUT,
  HTTPREQ_HEAD,
  HTTPREQ_OPTIONS,
  HTTPREQ_CUSTOM,
  HTTPREQ_LAST /* last in list */
} Curl_HttpReq;

typedef enum {
    RTSPREQ_NONE, /* first in list */
    RTSPREQ_OPTIONS,
    RTSPREQ_DESCRIBE,
    RTSPREQ_ANNOUNCE,
    RTSPREQ_SETUP,
    RTSPREQ_PLAY,
    RTSPREQ_PAUSE,
    RTSPREQ_TEARDOWN,
    RTSPREQ_GET_PARAMETER,
    RTSPREQ_SET_PARAMETER,
    RTSPREQ_RECORD,
    RTSPREQ_RECEIVE,
    RTSPREQ_LAST /* last in list */
} Curl_RtspReq;

/*
 * Values that are generated, temporary or calculated internally for a
 * "session handle" must be defined within the 'struct UrlState'.  This struct
 * will be used within the Curl_easy struct. When the 'Curl_easy'
 * struct is cloned, this data MUST NOT be copied.
 *
 * Remember that any "state" information goes globally for the curl handle.
 * Session-data MUST be put in the connectdata struct and here.  */
#define MAX_CURL_USER_LENGTH 256
#define MAX_CURL_PASSWORD_LENGTH 256

struct auth {
  unsigned long want;  /* Bitmask set to the authentication methods wanted by
                          app (with CURLOPT_HTTPAUTH or CURLOPT_PROXYAUTH). */
  unsigned long picked;
  unsigned long avail; /* Bitmask for what the server reports to support for
                          this resource */
  bool done;  /* TRUE when the auth phase is done and ready to do the *actual*
                 request */
  bool multipass; /* TRUE if this is not yet authenticated but within the
                     auth multipass negotiation */
  bool iestyle; /* TRUE if digest should be done IE-style or FALSE if it should
                   be RFC compliant */
};

struct Curl_http2_dep {
  struct Curl_http2_dep *next;
  struct Curl_easy *data;
};

/*
 * This struct is for holding data that was attempted to get sent to the user's
 * callback but is held due to pausing. One instance per type (BOTH, HEADER,
 * BODY).
 */
struct tempbuf {
  char *buf;  /* allocated buffer to keep data in when a write callback
                 returns to make the connection paused */
  size_t len; /* size of the 'tempwrite' allocated buffer */
  int type;   /* type of the 'tempwrite' buffer as a bitmask that is used with
                 Curl_client_write() */
};

/* Timers */
typedef enum {
  EXPIRE_100_TIMEOUT,
  EXPIRE_ASYNC_NAME,
  EXPIRE_CONNECTTIMEOUT,
  EXPIRE_DNS_PER_NAME,
  EXPIRE_HAPPY_EYEBALLS,
  EXPIRE_MULTI_PENDING,
  EXPIRE_RUN_NOW,
  EXPIRE_SPEEDCHECK,
  EXPIRE_TIMEOUT,
  EXPIRE_TOOFAST,
  EXPIRE_LAST /* not an actual timer, used as a marker only */
} expire_id;

/*
 * One instance for each timeout an easy handle can set.
 */
struct time_node {
  struct curl_llist_element list;
  struct curltime time;
  expire_id eid;
};

struct UrlState {

  /* Points to the connection cache */
  struct conncache *conn_cache;

  /* when curl_easy_perform() is called, the multi handle is "owned" by
     the easy handle so curl_easy_cleanup() on such an easy handle will
     also close the multi handle! */
  bool multi_owned_by_easy;

  /* buffers to store authentication data in, as parsed from input options */
  struct curltime keeps_speed; /* for the progress meter really */

  struct connectdata *lastconnect; /* The last connection, NULL if undefined */

  char *headerbuff; /* allocated buffer to store headers in */
  size_t headersize;   /* size of the allocation */

  char *buffer; /* download buffer */
  char uploadbuffer[UPLOAD_BUFSIZE + 1]; /* upload buffer */
  curl_off_t current_speed;  /* the ProgressShow() function sets this,
                                bytes / second */
  bool this_is_a_follow; /* this is a followed Location: request */
  bool refused_stream; /* this was refused, try again */
  char *first_host; /* host name of the first (not followed) request.
                       if set, this should be the host name that we will
                       sent authorization to, no else. Used to make Location:
                       following not keep sending user+password... This is
                       strdup() data.
                    */
  int first_remote_port; /* remote port of the first (not followed) request */
  struct curl_ssl_session *session; /* array of 'max_ssl_sessions' size */
  long sessionage;                  /* number of the most recent session */
  unsigned int tempcount; /* number of entries in use in tempwrite, 0 - 3 */
  struct tempbuf tempwrite[3]; /* BOTH, HEADER, BODY */
  char *scratch; /* huge buffer[set.buffer_size*2] for upload CRLF replacing */
  bool errorbuf; /* Set to TRUE if the error buffer is already filled in.
                    This must be set to FALSE every time _easy_perform() is
                    called. */
  int os_errno;  /* filled in with errno whenever an error occurs */
#ifdef HAVE_SIGNAL
  /* storage for the previous bag^H^H^HSIGPIPE signal handler :-) */
  void (*prev_signal)(int sig);
#endif
  bool allow_port; /* Is set.use_port allowed to take effect or not. This
                      is always set TRUE when curl_easy_perform() is called. */
  struct digestdata digest;      /* state data for host Digest auth */
  struct digestdata proxydigest; /* state data for proxy Digest auth */

#ifdef USE_SPNEGO
  struct negotiatedata negotiate; /* state data for host Negotiate auth */
  struct negotiatedata proxyneg; /* state data for proxy Negotiate auth */
#endif

  struct auth authhost;  /* auth details for host */
  struct auth authproxy; /* auth details for proxy */

  bool authproblem; /* TRUE if there's some problem authenticating */

  void *resolver; /* resolver state, if it is used in the URL state -
                     ares_channel f.e. */

#if defined(USE_OPENSSL) && defined(HAVE_OPENSSL_ENGINE_H)
  /* void instead of ENGINE to avoid bleeding OpenSSL into this header */
  void *engine;
#endif /* USE_OPENSSL */
  struct curltime expiretime; /* set this with Curl_expire() only */
  struct Curl_tree timenode; /* for the splay stuff */
  struct curl_llist timeoutlist; /* list of pending timeouts */
  struct time_node expires[EXPIRE_LAST]; /* nodes for each expire type */

  /* a place to store the most recently set FTP entrypath */
  char *most_recent_ftp_entrypath;

  /* set after initial USER failure, to prevent an authentication loop */
  bool ftp_trying_alternative;
  bool wildcardmatch; /* enable wildcard matching */
  int httpversion;       /* the lowest HTTP version*10 reported by any server
                            involved in this request */
  bool expect100header;  /* TRUE if we added Expect: 100-continue */

  bool pipe_broke; /* TRUE if the connection we were pipelined on broke
                      and we need to restart from the beginning */

#if !defined(WIN32) && !defined(MSDOS) && !defined(__EMX__) && \
    !defined(__SYMBIAN32__)
/* do FTP line-end conversions on most platforms */
#define CURL_DO_LINEEND_CONV
  /* for FTP downloads: track CRLF sequences that span blocks */
  bool prev_block_had_trailing_cr;
  /* for FTP downloads: how many CRLFs did we converted to LFs? */
  curl_off_t crlf_conversions;
#endif
  char *pathbuffer;/* allocated buffer to store the URL's path part in */
  char *path;      /* path to use, points to somewhere within the pathbuffer
                      area */
  bool slash_removed; /* set TRUE if the 'path' points to a path where the
                         initial URL slash separator has been taken off */
  bool use_range;
  bool rangestringalloc; /* the range string is malloc()'ed */

  char *range; /* range, if used. See README for detailed specification on
                  this syntax. */
  curl_off_t resume_from; /* continue [ftp] transfer from here */

  /* This RTSP state information survives requests and connections */
  long rtsp_next_client_CSeq; /* the session's next client CSeq */
  long rtsp_next_server_CSeq; /* the session's next server CSeq */
  long rtsp_CSeq_recv; /* most recent CSeq received */

  curl_off_t infilesize; /* size of file to upload, -1 means unknown.
                            Copied from set.filesize at start of operation */

  size_t drain; /* Increased when this stream has data to read, even if its
                   socket is not necessarily is readable. Decreased when
                   checked. */
  bool done; /* set to FALSE when Curl_init_do() is called and set to TRUE
                when multi_done() is called, to prevent multi_done() to get
                invoked twice when the multi interface is used. */

  curl_read_callback fread_func; /* read callback/function */
  void *in;                      /* CURLOPT_READDATA */

  struct Curl_easy *stream_depends_on;
  bool stream_depends_e; /* set or don't set the Exclusive bit */
  int stream_weight;
#ifdef CURLDEBUG
  bool conncache_lock;
#endif
};


/*
 * This 'DynamicStatic' struct defines dynamic states that actually change
 * values in the 'UserDefined' area, which MUST be taken into consideration
 * if the UserDefined struct is cloned or similar. You can probably just
 * copy these, but each one indicate a special action on other data.
 */

struct DynamicStatic {
  char *url;        /* work URL, copied from UserDefined */
  bool url_alloc;   /* URL string is malloc()'ed */
  char *referer;    /* referer string */
  bool referer_alloc; /* referer sting is malloc()ed */
  struct curl_slist *cookielist; /* list of cookie files set by
                                    curl_easy_setopt(COOKIEFILE) calls */
  struct curl_slist *resolve; /* set to point to the set.resolve list when
                                 this should be dealt with in pretransfer */
};

/*
 * This 'UserDefined' struct must only contain data that is set once to go
 * for many (perhaps) independent connections. Values that are generated or
 * calculated internally for the "session handle" MUST be defined within the
 * 'struct UrlState' instead. The only exceptions MUST note the changes in
 * the 'DynamicStatic' struct.
 * Character pointer fields point to dynamic storage, unless otherwise stated.
 */

struct Curl_multi;    /* declared and used only in multi.c */

enum dupstring {
  STRING_CERT_ORIG,       /* client certificate file name */
  STRING_CERT_PROXY,      /* client certificate file name */
  STRING_CERT_TYPE_ORIG,  /* format for certificate (default: PEM)*/
  STRING_CERT_TYPE_PROXY, /* format for certificate (default: PEM)*/
  STRING_COOKIE,          /* HTTP cookie string to send */
  STRING_COOKIEJAR,       /* dump all cookies to this file */
  STRING_CUSTOMREQUEST,   /* HTTP/FTP/RTSP request/method to use */
  STRING_DEFAULT_PROTOCOL, /* Protocol to use when the URL doesn't specify */
  STRING_DEVICE,          /* local network interface/address to use */
  STRING_ENCODING,        /* Accept-Encoding string */
  STRING_FTP_ACCOUNT,     /* ftp account data */
  STRING_FTP_ALTERNATIVE_TO_USER, /* command to send if USER/PASS fails */
  STRING_FTPPORT,         /* port to send with the FTP PORT command */
  STRING_KEY_ORIG,        /* private key file name */
  STRING_KEY_PROXY,       /* private key file name */
  STRING_KEY_PASSWD_ORIG, /* plain text private key password */
  STRING_KEY_PASSWD_PROXY, /* plain text private key password */
  STRING_KEY_TYPE_ORIG,   /* format for private key (default: PEM) */
  STRING_KEY_TYPE_PROXY,  /* format for private key (default: PEM) */
  STRING_KRB_LEVEL,       /* krb security level */
  STRING_NETRC_FILE,      /* if not NULL, use this instead of trying to find
                             $HOME/.netrc */
  STRING_PROXY,           /* proxy to use */
  STRING_PRE_PROXY,       /* pre socks proxy to use */
  STRING_SET_RANGE,       /* range, if used */
  STRING_SET_REFERER,     /* custom string for the HTTP referer field */
  STRING_SET_URL,         /* what original URL to work on */
  STRING_SSL_CAPATH_ORIG, /* CA directory name (doesn't work on windows) */
  STRING_SSL_CAPATH_PROXY, /* CA directory name (doesn't work on windows) */
  STRING_SSL_CAFILE_ORIG, /* certificate file to verify peer against */
  STRING_SSL_CAFILE_PROXY, /* certificate file to verify peer against */
  STRING_SSL_PINNEDPUBLICKEY_ORIG, /* public key file to verify peer against */
  STRING_SSL_PINNEDPUBLICKEY_PROXY, /* public key file to verify proxy */
  STRING_SSL_CIPHER_LIST_ORIG, /* list of ciphers to use */
  STRING_SSL_CIPHER_LIST_PROXY, /* list of ciphers to use */
  STRING_SSL_EGDSOCKET,   /* path to file containing the EGD daemon socket */
  STRING_SSL_RANDOM_FILE, /* path to file containing "random" data */
  STRING_USERAGENT,       /* User-Agent string */
  STRING_SSL_CRLFILE_ORIG, /* crl file to check certificate */
  STRING_SSL_CRLFILE_PROXY, /* crl file to check certificate */
  STRING_SSL_ISSUERCERT_ORIG, /* issuer cert file to check certificate */
  STRING_SSL_ISSUERCERT_PROXY, /* issuer cert file to check certificate */
  STRING_USERNAME,        /* <username>, if used */
  STRING_PASSWORD,        /* <password>, if used */
  STRING_OPTIONS,         /* <options>, if used */
  STRING_PROXYUSERNAME,   /* Proxy <username>, if used */
  STRING_PROXYPASSWORD,   /* Proxy <password>, if used */
  STRING_NOPROXY,         /* List of hosts which should not use the proxy, if
                             used */
  STRING_RTSP_SESSION_ID, /* Session ID to use */
  STRING_RTSP_STREAM_URI, /* Stream URI for this request */
  STRING_RTSP_TRANSPORT,  /* Transport for this session */
#if defined(USE_LIBSSH2) || defined(USE_LIBSSH)
  STRING_SSH_PRIVATE_KEY, /* path to the private key file for auth */
  STRING_SSH_PUBLIC_KEY,  /* path to the public key file for auth */
  STRING_SSH_HOST_PUBLIC_KEY_MD5, /* md5 of host public key in ascii hex */
  STRING_SSH_KNOWNHOSTS,  /* file name of knownhosts file */
#endif
  STRING_PROXY_SERVICE_NAME, /* Proxy service name */
  STRING_SERVICE_NAME,    /* Service name */
  STRING_MAIL_FROM,
  STRING_MAIL_AUTH,

#ifdef USE_TLS_SRP
  STRING_TLSAUTH_USERNAME_ORIG,  /* TLS auth <username> */
  STRING_TLSAUTH_USERNAME_PROXY, /* TLS auth <username> */
  STRING_TLSAUTH_PASSWORD_ORIG,  /* TLS auth <password> */
  STRING_TLSAUTH_PASSWORD_PROXY, /* TLS auth <password> */
#endif
  STRING_BEARER,                /* <bearer>, if used */
#ifdef USE_UNIX_SOCKETS
  STRING_UNIX_SOCKET_PATH,      /* path to Unix socket, if used */
#endif
  STRING_TARGET,                /* CURLOPT_REQUEST_TARGET */
  /* -- end of zero-terminated strings -- */

  STRING_LASTZEROTERMINATED,

  /* -- below this are pointers to binary data that cannot be strdup'ed.
     Each such pointer must be added manually to Curl_dupset() --- */

  STRING_COPYPOSTFIELDS,  /* if POST, set the fields' values here */

  STRING_LAST /* not used, just an end-of-list marker */
};

struct UserDefined {
  FILE *err;         /* the stderr user data goes here */
  void *debugdata;   /* the data that will be passed to fdebug */
  char *errorbuffer; /* (Static) store failure messages in here */
  long proxyport; /* If non-zero, use this port number by default. If the
                     proxy string features a ":[port]" that one will override
                     this. */
  void *out;         /* CURLOPT_WRITEDATA */
  void *in_set;      /* CURLOPT_READDATA */
  void *writeheader; /* write the header to this if non-NULL */
  void *rtp_out;     /* write RTP to this if non-NULL */
  long use_port;     /* which port to use (when not using default) */
  unsigned long httpauth;  /* kind of HTTP authentication to use (bitmask) */
  unsigned long proxyauth; /* kind of proxy authentication to use (bitmask) */
  unsigned long socks5auth;/* kind of SOCKS5 authentication to use (bitmask) */
  long followlocation; /* as in HTTP Location: */
  long maxredirs;    /* maximum no. of http(s) redirects to follow, set to -1
                        for infinity */

  int keep_post;     /* keep POSTs as POSTs after a 30x request; each
                        bit represents a request, from 301 to 303 */
  bool free_referer; /* set TRUE if 'referer' points to a string we
                        allocated */
  void *postfields;  /* if POST, set the fields' values here */
  curl_seek_callback seek_func;      /* function that seeks the input */
  curl_off_t postfieldsize; /* if POST, this might have a size to use instead
                               of strlen(), and then the data *may* be binary
                               (contain zero bytes) */
  unsigned short localport; /* local port number to bind to */
  int localportrange; /* number of additional port numbers to test in case the
                         'localport' one can't be bind()ed */
  curl_write_callback fwrite_func;   /* function that stores the output */
  curl_write_callback fwrite_header; /* function that stores headers */
  curl_write_callback fwrite_rtp;    /* function that stores interleaved RTP */
  curl_read_callback fread_func_set; /* function that reads the input */
  int is_fread_set; /* boolean, has read callback been set to non-NULL? */
  int is_fwrite_set; /* boolean, has write callback been set to non-NULL? */
  curl_progress_callback fprogress; /* OLD and deprecated progress callback  */
  curl_xferinfo_callback fxferinfo; /* progress callback */
  curl_debug_callback fdebug;      /* function that write informational data */
  curl_ioctl_callback ioctl_func;  /* function for I/O control */
  curl_sockopt_callback fsockopt;  /* function for setting socket options */
  void *sockopt_client; /* pointer to pass to the socket options callback */
  curl_opensocket_callback fopensocket; /* function for checking/translating
                                           the address and opening the
                                           socket */
  void *opensocket_client;
  curl_closesocket_callback fclosesocket; /* function for closing the
                                             socket */
  void *closesocket_client;

  void *seek_client;    /* pointer to pass to the seek callback */
  /* the 3 curl_conv_callback functions below are used on non-ASCII hosts */
  /* function to convert from the network encoding: */
  curl_conv_callback convfromnetwork;
  /* function to convert to the network encoding: */
  curl_conv_callback convtonetwork;
  /* function to convert from UTF-8 encoding: */
  curl_conv_callback convfromutf8;

  void *progress_client; /* pointer to pass to the progress callback */
  void *ioctl_client;   /* pointer to pass to the ioctl callback */
  long timeout;         /* in milliseconds, 0 means no timeout */
  long connecttimeout;  /* in milliseconds, 0 means no timeout */
  long accepttimeout;   /* in milliseconds, 0 means no timeout */
  long happy_eyeballs_timeout; /* in milliseconds, 0 is a valid value */
  long server_response_timeout; /* in milliseconds, 0 means no timeout */
  long tftp_blksize;    /* in bytes, 0 means use default */
  bool tftp_no_options; /* do not send TFTP options requests */
  curl_off_t filesize;  /* size of file to upload, -1 means unknown */
  long low_speed_limit; /* bytes/second */
  long low_speed_time;  /* number of seconds */
  curl_off_t max_send_speed; /* high speed limit in bytes/second for upload */
  curl_off_t max_recv_speed; /* high speed limit in bytes/second for
                                download */
  curl_off_t set_resume_from;  /* continue [ftp] transfer from here */
  struct curl_slist *headers; /* linked list of extra headers */
  struct curl_slist *proxyheaders; /* linked list of extra CONNECT headers */
  struct curl_httppost *httppost;  /* linked list of old POST data */
  curl_mimepart mimepost;  /* MIME/POST data. */
  bool sep_headers;     /* handle host and proxy headers separately */
  bool cookiesession;   /* new cookie session? */
  bool crlf;            /* convert crlf on ftp upload(?) */
  struct curl_slist *quote;     /* after connection is established */
  struct curl_slist *postquote; /* after the transfer */
  struct curl_slist *prequote; /* before the transfer, after type */
  struct curl_slist *source_quote;  /* 3rd party quote */
  struct curl_slist *source_prequote;  /* in 3rd party transfer mode - before
                                          the transfer on source host */
  struct curl_slist *source_postquote; /* in 3rd party transfer mode - after
                                          the transfer on source host */
  struct curl_slist *telnet_options; /* linked list of telnet options */
  struct curl_slist *resolve;     /* list of names to add/remove from
                                     DNS cache */
  struct curl_slist *connect_to; /* list of host:port mappings to override
                                    the hostname and port to connect to */
  curl_TimeCond timecondition; /* kind of time/date comparison */
  time_t timevalue;       /* what time to compare with */
  Curl_HttpReq httpreq;   /* what kind of HTTP request (if any) is this */
  long httpversion; /* when non-zero, a specific HTTP version requested to
                       be used in the library's request(s) */
  bool strip_path_slash; /* strip off initial slash from path */
  struct ssl_config_data ssl;  /* user defined SSL stuff */
  struct ssl_config_data proxy_ssl;  /* user defined SSL stuff for proxy */
  struct ssl_general_config general_ssl; /* general user defined SSL stuff */
  curl_proxytype proxytype; /* what kind of proxy that is in use */
  long dns_cache_timeout; /* DNS cache timeout */
  long buffer_size;      /* size of receive buffer to use */
  void *private_data; /* application-private data */

  struct curl_slist *http200aliases; /* linked list of aliases for http200 */

  long ipver; /* the CURL_IPRESOLVE_* defines in the public header file
                 0 - whatever, 1 - v2, 2 - v6 */

  curl_off_t max_filesize; /* Maximum file size to download */

  curl_ftpfile ftp_filemethod; /* how to get to a file when FTP is used  */

  int ftp_create_missing_dirs; /* 1 - create directories that don't exist
                                  2 - the same but also allow MKD to fail once
                               */

  curl_sshkeycallback ssh_keyfunc; /* key matching callback */
  void *ssh_keyfunc_userp;         /* custom pointer to callback */
  bool ssh_compression;            /* enable SSH compression */

/* Here follows boolean settings that define how to behave during
   this session. They are STATIC, set by libcurl users or at least initially
   and they don't change during operations. */

  bool printhost;        /* printing host name in debug info */
  bool get_filetime;     /* get the time and get of the remote file */
  bool tunnel_thru_httpproxy; /* use CONNECT through a HTTP proxy */
  bool prefer_ascii;     /* ASCII rather than binary */
  bool ftp_append;       /* append, not overwrite, on upload */
  bool ftp_list_only;    /* switch FTP command for listing directories */
  bool ftp_use_port;     /* use the FTP PORT command */
  bool hide_progress;    /* don't use the progress meter */
  bool http_fail_on_error;  /* fail on HTTP error codes >= 400 */
  bool http_keep_sending_on_error; /* for HTTP status codes >= 300 */
  bool http_follow_location; /* follow HTTP redirects */
  bool http_transfer_encoding; /* request compressed HTTP transfer-encoding */
  bool allow_auth_to_other_hosts;
  bool include_header;   /* include received protocol headers in data output */
  bool http_set_referer; /* is a custom referer used */
  bool http_auto_referer; /* set "correct" referer when following location: */
  bool opt_no_body;      /* as set with CURLOPT_NOBODY */
  bool upload;           /* upload request */
  enum CURL_NETRC_OPTION
       use_netrc;        /* defined in include/curl.h */
  bool verbose;          /* output verbosity */
  bool krb;              /* Kerberos connection requested */
  bool reuse_forbid;     /* forbidden to be reused, close after use */
  bool reuse_fresh;      /* do not re-use an existing connection  */
  bool ftp_use_epsv;     /* if EPSV is to be attempted or not */
  bool ftp_use_eprt;     /* if EPRT is to be attempted or not */
  bool ftp_use_pret;     /* if PRET is to be used before PASV or not */

  curl_usessl use_ssl;   /* if AUTH TLS is to be attempted etc, for FTP or
                            IMAP or POP3 or others! */
  curl_ftpauth ftpsslauth; /* what AUTH XXX to be attempted */
  curl_ftpccc ftp_ccc;   /* FTP CCC options */
  bool no_signal;        /* do not use any signal/alarm handler */
  bool global_dns_cache; /* subject for future removal */
  bool tcp_nodelay;      /* whether to enable TCP_NODELAY or not */
  bool ignorecl;         /* ignore content length */
  bool ftp_skip_ip;      /* skip the IP address the FTP server passes on to
                            us */
  bool connect_only;     /* make connection, let application use the socket */
  long ssh_auth_types;   /* allowed SSH auth types */
  bool http_te_skip;     /* pass the raw body data to the user, even when
                            transfer-encoded (chunked, compressed) */
  bool http_ce_skip;     /* pass the raw body data to the user, even when
                            content-encoded (chunked, compressed) */
  long new_file_perms;    /* Permissions to use when creating remote files */
  long new_directory_perms; /* Permissions to use when creating remote dirs */
  bool proxy_transfer_mode; /* set transfer mode (;type=<a|i>) when doing FTP
                               via an HTTP proxy */
  char *str[STRING_LAST]; /* array of strings, pointing to allocated memory */
  unsigned int scope_id;  /* Scope id for IPv6 */
  long allowed_protocols;
  long redir_protocols;
#if defined(HAVE_GSSAPI) || defined(USE_WINDOWS_SSPI)
  bool socks5_gssapi_nec; /* Flag to support NEC SOCKS5 server */
#endif
  struct curl_slist *mail_rcpt; /* linked list of mail recipients */
  bool sasl_ir;         /* Enable/disable SASL initial response */
  /* Common RTSP header options */
  Curl_RtspReq rtspreq; /* RTSP request type */
  long rtspversion; /* like httpversion, for RTSP */
  bool wildcard_enabled; /* enable wildcard matching */
  curl_chunk_bgn_callback chunk_bgn; /* called before part of transfer
                                        starts */
  curl_chunk_end_callback chunk_end; /* called after part transferring
                                        stopped */
  curl_fnmatch_callback fnmatch; /* callback to decide which file corresponds
                                    to pattern (e.g. if WILDCARDMATCH is on) */
  void *fnmatch_data;

  long gssapi_delegation; /* GSS-API credential delegation, see the
                             documentation of CURLOPT_GSSAPI_DELEGATION */

  bool tcp_keepalive;    /* use TCP keepalives */
  long tcp_keepidle;     /* seconds in idle before sending keepalive probe */
  long tcp_keepintvl;    /* seconds between TCP keepalive probes */
  bool tcp_fastopen;     /* use TCP Fast Open */

  size_t maxconnects;  /* Max idle connections in the connection cache */

  bool ssl_enable_npn;      /* TLS NPN extension? */
  bool ssl_enable_alpn;     /* TLS ALPN extension? */
  bool path_as_is;      /* allow dotdots? */
  bool pipewait;        /* wait for pipe/multiplex status before starting a
                           new connection */
  long expect_100_timeout; /* in milliseconds */
  bool suppress_connect_headers;  /* suppress proxy CONNECT response headers
                                     from user callbacks */

  bool dns_shuffle_addresses; /* whether to shuffle addresses before use */

  struct Curl_easy *stream_depends_on;
  bool stream_depends_e; /* set or don't set the Exclusive bit */
  int stream_weight;

  bool haproxyprotocol; /* whether to send HAProxy PROXY protocol header */

  struct Curl_http2_dep *stream_dependents;

  bool abstract_unix_socket;

  curl_resolver_start_callback resolver_start; /* optional callback called
                                                  before resolver start */
  void *resolver_start_client; /* pointer to pass to resolver start callback */
};

struct Names {
  struct curl_hash *hostcache;
  enum {
    HCACHE_NONE,    /* not pointing to anything */
    HCACHE_GLOBAL,  /* points to the (shrug) global one */
    HCACHE_MULTI,   /* points to a shared one in the multi handle */
    HCACHE_SHARED   /* points to a shared one in a shared object */
  } hostcachetype;
};

/*
 * The 'connectdata' struct MUST have all the connection oriented stuff as we
 * may have several simultaneous connections and connection structs in memory.
 *
 * The 'struct UserDefined' must only contain data that is set once to go for
 * many (perhaps) independent connections. Values that are generated or
 * calculated internally for the "session handle" must be defined within the
 * 'struct UrlState' instead.
 */

struct Curl_easy {
  /* first, two fields for the linked list of these */
  struct Curl_easy *next;
  struct Curl_easy *prev;

  struct connectdata *easy_conn;     /* the "unit's" connection */
  struct curl_llist_element connect_queue;
  struct curl_llist_element pipeline_queue;

  CURLMstate mstate;  /* the handle's state */
  CURLcode result;   /* previous result */

  struct Curl_message msg; /* A single posted message. */

  /* Array with the plain socket numbers this handle takes care of, in no
     particular order. Note that all sockets are added to the sockhash, where
     the state etc are also kept. This array is mostly used to detect when a
     socket is to be removed from the hash. See singlesocket(). */
  curl_socket_t sockets[MAX_SOCKSPEREASYHANDLE];
  int numsocks;

  struct Names dns;
  struct Curl_multi *multi;    /* if non-NULL, points to the multi handle
                                  struct to which this "belongs" when used by
                                  the multi interface */
  struct Curl_multi *multi_easy; /* if non-NULL, points to the multi handle
                                    struct to which this "belongs" when used
                                    by the easy interface */
  struct Curl_share *share;    /* Share, handles global variable mutexing */
  struct SingleRequest req;    /* Request-specific data */
  struct UserDefined set;      /* values set by the libcurl user */
  struct DynamicStatic change; /* possibly modified userdefined data */
  struct CookieInfo *cookies;  /* the cookies, read from files and servers.
                                  NOTE that the 'cookie' field in the
                                  UserDefined struct defines if the "engine"
                                  is to be used or not. */
  struct Progress progress;    /* for all the progress meter data */
  struct UrlState state;       /* struct for fields used for state info and
                                  other dynamic purposes */
  struct WildcardData wildcard; /* wildcard download state info */
  struct PureInfo info;        /* stats, reports and info data */
  struct curl_tlssessioninfo tsi; /* Information about the TLS session, only
                                     valid after a client has asked for it */
#if defined(CURL_DOES_CONVERSIONS) && defined(HAVE_ICONV)
  iconv_t outbound_cd;         /* for translating to the network encoding */
  iconv_t inbound_cd;          /* for translating from the network encoding */
  iconv_t utf8_cd;             /* for translating to UTF8 */
#endif /* CURL_DOES_CONVERSIONS && HAVE_ICONV */
  unsigned int magic;          /* set to a CURLEASY_MAGIC_NUMBER */
};

#define LIBCURL_NAME "libcurl"

#endif /* HEADER_CURL_URLDATA_H */
