#ifndef HEADER_CURL_HOSTIP_H
#define HEADER_CURL_HOSTIP_H
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

#include "curl_setup.h"
#include "hash.h"
#include "curl_addrinfo.h"
#include "asyn.h"

#ifdef HAVE_SETJMP_H
#include <setjmp.h>
#endif

#ifdef NETWARE
#undef in_addr_t
#define in_addr_t unsigned long
#endif

/* Allocate enough memory to hold the full name information structs and
 * everything. OSF1 is known to require at least 8872 bytes. The buffer
 * required for storing all possible aliases and IP numbers is according to
 * Stevens' Unix Network Programming 2nd edition, p. 304: 8192 bytes!
 */
#define CURL_HOSTENT_SIZE 9000

#define CURL_TIMEOUT_RESOLVE 300 /* when using asynch methods, we allow this
                                    many seconds for a name resolve */

#define CURL_ASYNC_SUCCESS CURLE_OK

struct addrinfo;
struct hostent;
struct Curl_easy;
struct connectdata;

/*
 * Curl_global_host_cache_init() initializes and sets up a global DNS cache.
 * Global DNS cache is general badness. Do not use. This will be removed in
 * a future version. Use the share interface instead!
 *
 * Returns a struct curl_hash pointer on success, NULL on failure.
 */
struct curl_hash *Curl_global_host_cache_init(void);
void Curl_global_host_cache_dtor(void);

struct Curl_dns_entry {
  Curl_addrinfo *addr;
  /* timestamp == 0 -- CURLOPT_RESOLVE entry, doesn't timeout */
  time_t timestamp;
  /* use-counter, use Curl_resolv_unlock to release reference */
  long inuse;
};

/*
 * Curl_resolv() returns an entry with the info for the specified host
 * and port.
 *
 * The returned data *MUST* be "unlocked" with Curl_resolv_unlock() after
 * use, or we'll leak memory!
 */
/* return codes */
#define CURLRESOLV_TIMEDOUT -2
#define CURLRESOLV_ERROR    -1
#define CURLRESOLV_RESOLVED  0
#define CURLRESOLV_PENDING   1
int Curl_resolv(struct connectdata *conn, const char *hostname,
                int port, struct Curl_dns_entry **dnsentry);
int Curl_resolv_timeout(struct connectdata *conn, const char *hostname,
                        int port, struct Curl_dns_entry **dnsentry,
                        time_t timeoutms);

#ifdef CURLRES_IPV6
/*
 * Curl_ipv6works() returns TRUE if IPv6 seems to work.
 */
bool Curl_ipv6works(void);
#else
#define Curl_ipv6works() FALSE
#endif

/*
 * Curl_ipvalid() checks what CURL_IPRESOLVE_* requirements that might've
 * been set and returns TRUE if they are OK.
 */
bool Curl_ipvalid(struct connectdata *conn);


/*
 * Curl_getaddrinfo() is the generic low-level name resolve API within this
 * source file. There are several versions of this function - for different
 * name resolve layers (selected at build-time). They all take this same set
 * of arguments
 */
Curl_addrinfo *Curl_getaddrinfo(struct connectdata *conn,
                                const char *hostname,
                                int port,
                                int *waitp);


/* unlock a previously resolved dns entry */
void Curl_resolv_unlock(struct Curl_easy *data,
                        struct Curl_dns_entry *dns);

/* for debugging purposes only: */
void Curl_scan_cache_used(void *user, void *ptr);

/* init a new dns cache and return success */
int Curl_mk_dnscache(struct curl_hash *hash);

/* prune old entries from the DNS cache */
void Curl_hostcache_prune(struct Curl_easy *data);

/* Return # of addresses in a Curl_addrinfo struct */
int Curl_num_addresses(const Curl_addrinfo *addr);

#if defined(CURLDEBUG) && defined(HAVE_GETNAMEINFO)
int curl_dogetnameinfo(GETNAMEINFO_QUAL_ARG1 GETNAMEINFO_TYPE_ARG1 sa,
                       GETNAMEINFO_TYPE_ARG2 salen,
                       char *host, GETNAMEINFO_TYPE_ARG46 hostlen,
                       char *serv, GETNAMEINFO_TYPE_ARG46 servlen,
                       GETNAMEINFO_TYPE_ARG7 flags,
                       int line, const char *source);
#endif

/* IPv4 threadsafe resolve function used for synch and asynch builds */
Curl_addrinfo *Curl_ipv4_resolve_r(const char *hostname, int port);

CURLcode Curl_async_resolved(struct connectdata *conn,
                             bool *protocol_connect);

#ifndef CURLRES_ASYNCH
#define Curl_async_resolved(x,y) CURLE_OK
#endif

/*
 * Curl_addrinfo_callback() is used when we build with any asynch specialty.
 * Handles end of async request processing. Inserts ai into hostcache when
 * status is CURL_ASYNC_SUCCESS. Twiddles fields in conn to indicate async
 * request completed whether successful or failed.
 */
CURLcode Curl_addrinfo_callback(struct connectdata *conn,
                                int status,
                                Curl_addrinfo *ai);

/*
 * Curl_printable_address() returns a printable version of the 1st address
 * given in the 'ip' argument. The result will be stored in the buf that is
 * bufsize bytes big.
 */
const char *Curl_printable_address(const Curl_addrinfo *ip,
                                   char *buf, size_t bufsize);

/*
 * Curl_fetch_addr() fetches a 'Curl_dns_entry' already in the DNS cache.
 *
 * Returns the Curl_dns_entry entry pointer or NULL if not in the cache.
 *
 * The returned data *MUST* be "unlocked" with Curl_resolv_unlock() after
 * use, or we'll leak memory!
 */
struct Curl_dns_entry *
Curl_fetch_addr(struct connectdata *conn,
                const char *hostname,
                int port);

/*
 * Curl_shuffle_addr() shuffles the order of addresses in a 'Curl_addrinfo'
 * struct by re-linking its linked list.
 *
 * The addr argument should be the address of a pointer to the head node of a
 * `Curl_addrinfo` list and it will be modified to point to the new head after
 * shuffling.
 */
CURLcode Curl_shuffle_addr(struct Curl_easy *data, Curl_addrinfo **addr);

/*
 * Curl_cache_addr() stores a 'Curl_addrinfo' struct in the DNS cache.
 *
 * Returns the Curl_dns_entry entry pointer or NULL if the storage failed.
 */
struct Curl_dns_entry *
Curl_cache_addr(struct Curl_easy *data, Curl_addrinfo *addr,
                const char *hostname, int port);

#ifndef INADDR_NONE
#define CURL_INADDR_NONE (in_addr_t) ~0
#else
#define CURL_INADDR_NONE INADDR_NONE
#endif

#ifdef HAVE_SIGSETJMP
/* Forward-declaration of variable defined in hostip.c. Beware this
 * is a global and unique instance. This is used to store the return
 * address that we can jump back to from inside a signal handler.
 * This is not thread-safe stuff.
 */
extern sigjmp_buf curl_jmpenv;
#endif

/*
 * Function provided by the resolver backend to set DNS servers to use.
 */
CURLcode Curl_set_dns_servers(struct Curl_easy *data, char *servers);

/*
 * Function provided by the resolver backend to set
 * outgoing interface to use for DNS requests
 */
CURLcode Curl_set_dns_interface(struct Curl_easy *data,
                                const char *interf);

/*
 * Function provided by the resolver backend to set
 * local IPv4 address to use as source address for DNS requests
 */
CURLcode Curl_set_dns_local_ip4(struct Curl_easy *data,
                                const char *local_ip4);

/*
 * Function provided by the resolver backend to set
 * local IPv6 address to use as source address for DNS requests
 */
CURLcode Curl_set_dns_local_ip6(struct Curl_easy *data,
                                const char *local_ip6);

/*
 * Clean off entries from the cache
 */
void Curl_hostcache_clean(struct Curl_easy *data, struct curl_hash *hash);

/*
 * Destroy the hostcache of this handle.
 */
void Curl_hostcache_destroy(struct Curl_easy *data);

/*
 * Populate the cache with specified entries from CURLOPT_RESOLVE.
 */
CURLcode Curl_loadhostpairs(struct Curl_easy *data);

#endif /* HEADER_CURL_HOSTIP_H */
