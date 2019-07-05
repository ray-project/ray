from __future__ import unicode_literals
from itertools import chain
import datetime
import sys
import warnings
import time
import threading
import time as mod_time
import hashlib
from redis._compat import (basestring, imap, iteritems, iterkeys,
                           itervalues, izip, long, nativestr, safe_unicode)
from redis.connection import (ConnectionPool, UnixDomainSocketConnection,
                              SSLConnection, Token)
from redis.lock import Lock
from redis.exceptions import (
    ConnectionError,
    DataError,
    ExecAbortError,
    NoScriptError,
    PubSubError,
    RedisError,
    ResponseError,
    TimeoutError,
    WatchError,
)

SYM_EMPTY = b''
EMPTY_RESPONSE = 'EMPTY_RESPONSE'


def list_or_args(keys, args):
    # returns a single new list combining keys and args
    try:
        iter(keys)
        # a string or bytes instance can be iterated, but indicates
        # keys wasn't passed as a list
        if isinstance(keys, (basestring, bytes)):
            keys = [keys]
        else:
            keys = list(keys)
    except TypeError:
        keys = [keys]
    if args:
        keys.extend(args)
    return keys


def timestamp_to_datetime(response):
    "Converts a unix timestamp to a Python datetime object"
    if not response:
        return None
    try:
        response = int(response)
    except ValueError:
        return None
    return datetime.datetime.fromtimestamp(response)


def string_keys_to_dict(key_string, callback):
    return dict.fromkeys(key_string.split(), callback)


def dict_merge(*dicts):
    merged = {}
    for d in dicts:
        merged.update(d)
    return merged


def parse_debug_object(response):
    "Parse the results of Redis's DEBUG OBJECT command into a Python dict"
    # The 'type' of the object is the first item in the response, but isn't
    # prefixed with a name
    response = nativestr(response)
    response = 'type:' + response
    response = dict(kv.split(':') for kv in response.split())

    # parse some expected int values from the string response
    # note: this cmd isn't spec'd so these may not appear in all redis versions
    int_fields = ('refcount', 'serializedlength', 'lru', 'lru_seconds_idle')
    for field in int_fields:
        if field in response:
            response[field] = int(response[field])

    return response


def parse_object(response, infotype):
    "Parse the results of an OBJECT command"
    if infotype in ('idletime', 'refcount'):
        return int_or_none(response)
    return response


def parse_info(response):
    "Parse the result of Redis's INFO command into a Python dict"
    info = {}
    response = nativestr(response)

    def get_value(value):
        if ',' not in value or '=' not in value:
            try:
                if '.' in value:
                    return float(value)
                else:
                    return int(value)
            except ValueError:
                return value
        else:
            sub_dict = {}
            for item in value.split(','):
                k, v = item.rsplit('=', 1)
                sub_dict[k] = get_value(v)
            return sub_dict

    for line in response.splitlines():
        if line and not line.startswith('#'):
            if line.find(':') != -1:
                # Split, the info fields keys and values.
                # Note that the value may contain ':'. but the 'host:'
                # pseudo-command is the only case where the key contains ':'
                key, value = line.split(':', 1)
                if key == 'cmdstat_host':
                    key, value = line.rsplit(':', 1)
                info[key] = get_value(value)
            else:
                # if the line isn't splittable, append it to the "__raw__" key
                info.setdefault('__raw__', []).append(line)

    return info


SENTINEL_STATE_TYPES = {
    'can-failover-its-master': int,
    'config-epoch': int,
    'down-after-milliseconds': int,
    'failover-timeout': int,
    'info-refresh': int,
    'last-hello-message': int,
    'last-ok-ping-reply': int,
    'last-ping-reply': int,
    'last-ping-sent': int,
    'master-link-down-time': int,
    'master-port': int,
    'num-other-sentinels': int,
    'num-slaves': int,
    'o-down-time': int,
    'pending-commands': int,
    'parallel-syncs': int,
    'port': int,
    'quorum': int,
    'role-reported-time': int,
    's-down-time': int,
    'slave-priority': int,
    'slave-repl-offset': int,
    'voted-leader-epoch': int
}


def parse_sentinel_state(item):
    result = pairs_to_dict_typed(item, SENTINEL_STATE_TYPES)
    flags = set(result['flags'].split(','))
    for name, flag in (('is_master', 'master'), ('is_slave', 'slave'),
                       ('is_sdown', 's_down'), ('is_odown', 'o_down'),
                       ('is_sentinel', 'sentinel'),
                       ('is_disconnected', 'disconnected'),
                       ('is_master_down', 'master_down')):
        result[name] = flag in flags
    return result


def parse_sentinel_master(response):
    return parse_sentinel_state(imap(nativestr, response))


def parse_sentinel_masters(response):
    result = {}
    for item in response:
        state = parse_sentinel_state(imap(nativestr, item))
        result[state['name']] = state
    return result


def parse_sentinel_slaves_and_sentinels(response):
    return [parse_sentinel_state(imap(nativestr, item)) for item in response]


def parse_sentinel_get_master(response):
    return response and (response[0], int(response[1])) or None


def pairs_to_dict(response, decode_keys=False):
    "Create a dict given a list of key/value pairs"
    if response is None:
        return {}
    if decode_keys:
        # the iter form is faster, but I don't know how to make that work
        # with a nativestr() map
        return dict(izip(imap(nativestr, response[::2]), response[1::2]))
    else:
        it = iter(response)
        return dict(izip(it, it))


def pairs_to_dict_typed(response, type_info):
    it = iter(response)
    result = {}
    for key, value in izip(it, it):
        if key in type_info:
            try:
                value = type_info[key](value)
            except Exception:
                # if for some reason the value can't be coerced, just use
                # the string value
                pass
        result[key] = value
    return result


def zset_score_pairs(response, **options):
    """
    If ``withscores`` is specified in the options, return the response as
    a list of (value, score) pairs
    """
    if not response or not options.get('withscores'):
        return response
    score_cast_func = options.get('score_cast_func', float)
    it = iter(response)
    return list(izip(it, imap(score_cast_func, it)))


def sort_return_tuples(response, **options):
    """
    If ``groups`` is specified, return the response as a list of
    n-element tuples with n being the value found in options['groups']
    """
    if not response or not options.get('groups'):
        return response
    n = options['groups']
    return list(izip(*[response[i::n] for i in range(n)]))


def int_or_none(response):
    if response is None:
        return None
    return int(response)


def nativestr_or_none(response):
    if response is None:
        return None
    return nativestr(response)


def parse_stream_list(response):
    if response is None:
        return None
    return [(r[0], pairs_to_dict(r[1])) for r in response]


def pairs_to_dict_with_nativestr_keys(response):
    return pairs_to_dict(response, decode_keys=True)


def parse_list_of_dicts(response):
    return list(imap(pairs_to_dict_with_nativestr_keys, response))


def parse_xclaim(response, **options):
    if options.get('parse_justid', False):
        return response
    return parse_stream_list(response)


def parse_xinfo_stream(response):
    data = pairs_to_dict(response, decode_keys=True)
    first = data['first-entry']
    data['first-entry'] = (first[0], pairs_to_dict(first[1]))
    last = data['last-entry']
    data['last-entry'] = (last[0], pairs_to_dict(last[1]))
    return data


def parse_xread(response):
    if response is None:
        return []
    return [[r[0], parse_stream_list(r[1])] for r in response]


def parse_xpending(response, **options):
    if options.get('parse_detail', False):
        return parse_xpending_range(response)
    consumers = [{'name': n, 'pending': long(p)} for n, p in response[3] or []]
    return {
        'pending': response[0],
        'min': response[1],
        'max': response[2],
        'consumers': consumers
    }


def parse_xpending_range(response):
    k = ('message_id', 'consumer', 'time_since_delivered', 'times_delivered')
    return [dict(izip(k, r)) for r in response]


def float_or_none(response):
    if response is None:
        return None
    return float(response)


def bool_ok(response):
    return nativestr(response) == 'OK'


def parse_zadd(response, **options):
    if response is None:
        return None
    if options.get('as_score'):
        return float(response)
    return int(response)


def parse_client_list(response, **options):
    clients = []
    for c in nativestr(response).splitlines():
        # Values might contain '='
        clients.append(dict(pair.split('=', 1) for pair in c.split(' ')))
    return clients


def parse_config_get(response, **options):
    response = [nativestr(i) if i is not None else None for i in response]
    return response and pairs_to_dict(response) or {}


def parse_scan(response, **options):
    cursor, r = response
    return long(cursor), r


def parse_hscan(response, **options):
    cursor, r = response
    return long(cursor), r and pairs_to_dict(r) or {}


def parse_zscan(response, **options):
    score_cast_func = options.get('score_cast_func', float)
    cursor, r = response
    it = iter(r)
    return long(cursor), list(izip(it, imap(score_cast_func, it)))


def parse_slowlog_get(response, **options):
    return [{
        'id': item[0],
        'start_time': int(item[1]),
        'duration': int(item[2]),
        'command': b' '.join(item[3])
    } for item in response]


def parse_cluster_info(response, **options):
    response = nativestr(response)
    return dict(line.split(':') for line in response.splitlines() if line)


def _parse_node_line(line):
    line_items = line.split(' ')
    node_id, addr, flags, master_id, ping, pong, epoch, \
        connected = line.split(' ')[:8]
    slots = [sl.split('-') for sl in line_items[8:]]
    node_dict = {
        'node_id': node_id,
        'flags': flags,
        'master_id': master_id,
        'last_ping_sent': ping,
        'last_pong_rcvd': pong,
        'epoch': epoch,
        'slots': slots,
        'connected': True if connected == 'connected' else False
    }
    return addr, node_dict


def parse_cluster_nodes(response, **options):
    response = nativestr(response)
    raw_lines = response
    if isinstance(response, basestring):
        raw_lines = response.splitlines()
    return dict(_parse_node_line(line) for line in raw_lines)


def parse_georadius_generic(response, **options):
    if options['store'] or options['store_dist']:
        # `store` and `store_diff` cant be combined
        # with other command arguments.
        return response

    if type(response) != list:
        response_list = [response]
    else:
        response_list = response

    if not options['withdist'] and not options['withcoord']\
            and not options['withhash']:
        # just a bunch of places
        return response_list

    cast = {
        'withdist': float,
        'withcoord': lambda ll: (float(ll[0]), float(ll[1])),
        'withhash': int
    }

    # zip all output results with each casting functino to get
    # the properly native Python value.
    f = [lambda x: x]
    f += [cast[o] for o in ['withdist', 'withhash', 'withcoord'] if options[o]]
    return [
        list(map(lambda fv: fv[0](fv[1]), zip(f, r))) for r in response_list
    ]


def parse_pubsub_numsub(response, **options):
    return list(zip(response[0::2], response[1::2]))


def parse_client_kill(response, **options):
    if isinstance(response, (long, int)):
        return int(response)
    return nativestr(response) == 'OK'


class Redis(object):
    """
    Implementation of the Redis protocol.

    This abstract class provides a Python interface to all Redis commands
    and an implementation of the Redis protocol.

    Connection and Pipeline derive from this, implementing how
    the commands are sent and received to the Redis server
    """
    RESPONSE_CALLBACKS = dict_merge(
        string_keys_to_dict(
            'AUTH EXPIRE EXPIREAT HEXISTS HMSET MOVE MSETNX PERSIST '
            'PSETEX RENAMENX SISMEMBER SMOVE SETEX SETNX',
            bool
        ),
        string_keys_to_dict(
            'BITCOUNT BITPOS DECRBY DEL EXISTS GEOADD GETBIT HDEL HLEN '
            'HSTRLEN INCRBY LINSERT LLEN LPUSHX PFADD PFCOUNT RPUSHX SADD '
            'SCARD SDIFFSTORE SETBIT SETRANGE SINTERSTORE SREM STRLEN '
            'SUNIONSTORE UNLINK XACK XDEL XLEN XTRIM ZCARD ZLEXCOUNT ZREM '
            'ZREMRANGEBYLEX ZREMRANGEBYRANK ZREMRANGEBYSCORE',
            int
        ),
        string_keys_to_dict(
            'INCRBYFLOAT HINCRBYFLOAT',
            float
        ),
        string_keys_to_dict(
            # these return OK, or int if redis-server is >=1.3.4
            'LPUSH RPUSH',
            lambda r: isinstance(r, (long, int)) and r or nativestr(r) == 'OK'
        ),
        string_keys_to_dict('SORT', sort_return_tuples),
        string_keys_to_dict('ZSCORE ZINCRBY GEODIST', float_or_none),
        string_keys_to_dict(
            'FLUSHALL FLUSHDB LSET LTRIM MSET PFMERGE RENAME '
            'SAVE SELECT SHUTDOWN SLAVEOF SWAPDB WATCH UNWATCH',
            bool_ok
        ),
        string_keys_to_dict('BLPOP BRPOP', lambda r: r and tuple(r) or None),
        string_keys_to_dict(
            'SDIFF SINTER SMEMBERS SUNION',
            lambda r: r and set(r) or set()
        ),
        string_keys_to_dict(
            'ZPOPMAX ZPOPMIN ZRANGE ZRANGEBYSCORE ZREVRANGE ZREVRANGEBYSCORE',
            zset_score_pairs
        ),
        string_keys_to_dict('BZPOPMIN BZPOPMAX', \
                            lambda r: r and (r[0], r[1], float(r[2])) or None),
        string_keys_to_dict('ZRANK ZREVRANK', int_or_none),
        string_keys_to_dict('XREVRANGE XRANGE', parse_stream_list),
        string_keys_to_dict('XREAD XREADGROUP', parse_xread),
        string_keys_to_dict('BGREWRITEAOF BGSAVE', lambda r: True),
        {
            'CLIENT GETNAME': lambda r: r and nativestr(r),
            'CLIENT ID': int,
            'CLIENT KILL': parse_client_kill,
            'CLIENT LIST': parse_client_list,
            'CLIENT SETNAME': bool_ok,
            'CLIENT UNBLOCK': lambda r: r and int(r) == 1 or False,
            'CLIENT PAUSE': bool_ok,
            'CLUSTER ADDSLOTS': bool_ok,
            'CLUSTER COUNT-FAILURE-REPORTS': lambda x: int(x),
            'CLUSTER COUNTKEYSINSLOT': lambda x: int(x),
            'CLUSTER DELSLOTS': bool_ok,
            'CLUSTER FAILOVER': bool_ok,
            'CLUSTER FORGET': bool_ok,
            'CLUSTER INFO': parse_cluster_info,
            'CLUSTER KEYSLOT': lambda x: int(x),
            'CLUSTER MEET': bool_ok,
            'CLUSTER NODES': parse_cluster_nodes,
            'CLUSTER REPLICATE': bool_ok,
            'CLUSTER RESET': bool_ok,
            'CLUSTER SAVECONFIG': bool_ok,
            'CLUSTER SET-CONFIG-EPOCH': bool_ok,
            'CLUSTER SETSLOT': bool_ok,
            'CLUSTER SLAVES': parse_cluster_nodes,
            'CONFIG GET': parse_config_get,
            'CONFIG RESETSTAT': bool_ok,
            'CONFIG SET': bool_ok,
            'DEBUG OBJECT': parse_debug_object,
            'GEOHASH': lambda r: list(map(nativestr_or_none, r)),
            'GEOPOS': lambda r: list(map(lambda ll: (float(ll[0]),
                                         float(ll[1]))
                                         if ll is not None else None, r)),
            'GEORADIUS': parse_georadius_generic,
            'GEORADIUSBYMEMBER': parse_georadius_generic,
            'HGETALL': lambda r: r and pairs_to_dict(r) or {},
            'HSCAN': parse_hscan,
            'INFO': parse_info,
            'LASTSAVE': timestamp_to_datetime,
            'MEMORY PURGE': bool_ok,
            'MEMORY USAGE': int_or_none,
            'OBJECT': parse_object,
            'PING': lambda r: nativestr(r) == 'PONG',
            'PUBSUB NUMSUB': parse_pubsub_numsub,
            'RANDOMKEY': lambda r: r and r or None,
            'SCAN': parse_scan,
            'SCRIPT EXISTS': lambda r: list(imap(bool, r)),
            'SCRIPT FLUSH': bool_ok,
            'SCRIPT KILL': bool_ok,
            'SCRIPT LOAD': nativestr,
            'SENTINEL GET-MASTER-ADDR-BY-NAME': parse_sentinel_get_master,
            'SENTINEL MASTER': parse_sentinel_master,
            'SENTINEL MASTERS': parse_sentinel_masters,
            'SENTINEL MONITOR': bool_ok,
            'SENTINEL REMOVE': bool_ok,
            'SENTINEL SENTINELS': parse_sentinel_slaves_and_sentinels,
            'SENTINEL SET': bool_ok,
            'SENTINEL SLAVES': parse_sentinel_slaves_and_sentinels,
            'SET': lambda r: r and nativestr(r) == 'OK',
            'SLOWLOG GET': parse_slowlog_get,
            'SLOWLOG LEN': int,
            'SLOWLOG RESET': bool_ok,
            'SSCAN': parse_scan,
            'TIME': lambda x: (int(x[0]), int(x[1])),
            'XCLAIM': parse_xclaim,
            'XGROUP CREATE': bool_ok,
            'XGROUP DELCONSUMER': int,
            'XGROUP DESTROY': bool,
            'XGROUP SETID': bool_ok,
            'XINFO CONSUMERS': parse_list_of_dicts,
            'XINFO GROUPS': parse_list_of_dicts,
            'XINFO STREAM': parse_xinfo_stream,
            'XPENDING': parse_xpending,
            'ZADD': parse_zadd,
            'ZSCAN': parse_zscan,
        }
    )

    @classmethod
    def from_url(cls, url, db=None, **kwargs):
        """
        Return a Redis client object configured from the given URL

        For example::

            redis://[:password]@localhost:6379/0
            rediss://[:password]@localhost:6379/0
            unix://[:password]@/path/to/socket.sock?db=0

        Three URL schemes are supported:

        - ```redis://``
          <http://www.iana.org/assignments/uri-schemes/prov/redis>`_ creates a
          normal TCP socket connection
        - ```rediss://``
          <http://www.iana.org/assignments/uri-schemes/prov/rediss>`_ creates a
          SSL wrapped TCP socket connection
        - ``unix://`` creates a Unix Domain Socket connection

        There are several ways to specify a database number. The parse function
        will return the first specified option:
            1. A ``db`` querystring option, e.g. redis://localhost?db=0
            2. If using the redis:// scheme, the path argument of the url, e.g.
               redis://localhost/0
            3. The ``db`` argument to this function.

        If none of these options are specified, db=0 is used.

        Any additional querystring arguments and keyword arguments will be
        passed along to the ConnectionPool class's initializer. In the case
        of conflicting arguments, querystring arguments always win.
        """
        connection_pool = ConnectionPool.from_url(url, db=db, **kwargs)
        return cls(connection_pool=connection_pool)

    def __init__(self, host='localhost', port=6379,
                 db=0, password=None, socket_timeout=None,
                 socket_connect_timeout=None,
                 socket_keepalive=None, socket_keepalive_options=None,
                 connection_pool=None, unix_socket_path=None,
                 encoding='utf-8', encoding_errors='strict',
                 charset=None, errors=None,
                 decode_responses=False, retry_on_timeout=False,
                 ssl=False, ssl_keyfile=None, ssl_certfile=None,
                 ssl_cert_reqs='required', ssl_ca_certs=None,
                 max_connections=None):
        if not connection_pool:
            if charset is not None:
                warnings.warn(DeprecationWarning(
                    '"charset" is deprecated. Use "encoding" instead'))
                encoding = charset
            if errors is not None:
                warnings.warn(DeprecationWarning(
                    '"errors" is deprecated. Use "encoding_errors" instead'))
                encoding_errors = errors

            kwargs = {
                'db': db,
                'password': password,
                'socket_timeout': socket_timeout,
                'encoding': encoding,
                'encoding_errors': encoding_errors,
                'decode_responses': decode_responses,
                'retry_on_timeout': retry_on_timeout,
                'max_connections': max_connections
            }
            # based on input, setup appropriate connection args
            if unix_socket_path is not None:
                kwargs.update({
                    'path': unix_socket_path,
                    'connection_class': UnixDomainSocketConnection
                })
            else:
                # TCP specific options
                kwargs.update({
                    'host': host,
                    'port': port,
                    'socket_connect_timeout': socket_connect_timeout,
                    'socket_keepalive': socket_keepalive,
                    'socket_keepalive_options': socket_keepalive_options,
                })

                if ssl:
                    kwargs.update({
                        'connection_class': SSLConnection,
                        'ssl_keyfile': ssl_keyfile,
                        'ssl_certfile': ssl_certfile,
                        'ssl_cert_reqs': ssl_cert_reqs,
                        'ssl_ca_certs': ssl_ca_certs,
                    })
            connection_pool = ConnectionPool(**kwargs)
        self.connection_pool = connection_pool

        self.response_callbacks = self.__class__.RESPONSE_CALLBACKS.copy()

    def __repr__(self):
        return "%s<%s>" % (type(self).__name__, repr(self.connection_pool))

    def set_response_callback(self, command, callback):
        "Set a custom Response Callback"
        self.response_callbacks[command] = callback

    def pipeline(self, transaction=True, shard_hint=None):
        """
        Return a new pipeline object that can queue multiple commands for
        later execution. ``transaction`` indicates whether all commands
        should be executed atomically. Apart from making a group of operations
        atomic, pipelines are useful for reducing the back-and-forth overhead
        between the client and server.
        """
        return Pipeline(
            self.connection_pool,
            self.response_callbacks,
            transaction,
            shard_hint)

    def transaction(self, func, *watches, **kwargs):
        """
        Convenience method for executing the callable `func` as a transaction
        while watching all keys specified in `watches`. The 'func' callable
        should expect a single argument which is a Pipeline object.
        """
        shard_hint = kwargs.pop('shard_hint', None)
        value_from_callable = kwargs.pop('value_from_callable', False)
        watch_delay = kwargs.pop('watch_delay', None)
        with self.pipeline(True, shard_hint) as pipe:
            while True:
                try:
                    if watches:
                        pipe.watch(*watches)
                    func_value = func(pipe)
                    exec_value = pipe.execute()
                    return func_value if value_from_callable else exec_value
                except WatchError:
                    if watch_delay is not None and watch_delay > 0:
                        time.sleep(watch_delay)
                    continue

    def lock(self, name, timeout=None, sleep=0.1, blocking_timeout=None,
             lock_class=None, thread_local=True):
        """
        Return a new Lock object using key ``name`` that mimics
        the behavior of threading.Lock.

        If specified, ``timeout`` indicates a maximum life for the lock.
        By default, it will remain locked until release() is called.

        ``sleep`` indicates the amount of time to sleep per loop iteration
        when the lock is in blocking mode and another client is currently
        holding the lock.

        ``blocking_timeout`` indicates the maximum amount of time in seconds to
        spend trying to acquire the lock. A value of ``None`` indicates
        continue trying forever. ``blocking_timeout`` can be specified as a
        float or integer, both representing the number of seconds to wait.

        ``lock_class`` forces the specified lock implementation.

        ``thread_local`` indicates whether the lock token is placed in
        thread-local storage. By default, the token is placed in thread local
        storage so that a thread only sees its token, not a token set by
        another thread. Consider the following timeline:

            time: 0, thread-1 acquires `my-lock`, with a timeout of 5 seconds.
                     thread-1 sets the token to "abc"
            time: 1, thread-2 blocks trying to acquire `my-lock` using the
                     Lock instance.
            time: 5, thread-1 has not yet completed. redis expires the lock
                     key.
            time: 5, thread-2 acquired `my-lock` now that it's available.
                     thread-2 sets the token to "xyz"
            time: 6, thread-1 finishes its work and calls release(). if the
                     token is *not* stored in thread local storage, then
                     thread-1 would see the token value as "xyz" and would be
                     able to successfully release the thread-2's lock.

        In some use cases it's necessary to disable thread local storage. For
        example, if you have code where one thread acquires a lock and passes
        that lock instance to a worker thread to release later. If thread
        local storage isn't disabled in this case, the worker thread won't see
        the token set by the thread that acquired the lock. Our assumption
        is that these cases aren't common and as such default to using
        thread local storage.        """
        if lock_class is None:
            lock_class = Lock
        return lock_class(self, name, timeout=timeout, sleep=sleep,
                          blocking_timeout=blocking_timeout,
                          thread_local=thread_local)

    def pubsub(self, **kwargs):
        """
        Return a Publish/Subscribe object. With this object, you can
        subscribe to channels and listen for messages that get published to
        them.
        """
        return PubSub(self.connection_pool, **kwargs)

    # COMMAND EXECUTION AND PROTOCOL PARSING
    def execute_command(self, *args, **options):
        "Execute a command and return a parsed response"
        pool = self.connection_pool
        command_name = args[0]
        connection = pool.get_connection(command_name, **options)
        try:
            connection.send_command(*args)
            return self.parse_response(connection, command_name, **options)
        except (ConnectionError, TimeoutError) as e:
            connection.disconnect()
            if not (connection.retry_on_timeout and
                    isinstance(e, TimeoutError)):
                raise
            connection.send_command(*args)
            return self.parse_response(connection, command_name, **options)
        finally:
            pool.release(connection)

    def parse_response(self, connection, command_name, **options):
        "Parses a response from the Redis server"
        try:
            response = connection.read_response()
        except ResponseError:
            if EMPTY_RESPONSE in options:
                return options[EMPTY_RESPONSE]
            raise
        if command_name in self.response_callbacks:
            return self.response_callbacks[command_name](response, **options)
        return response

    # SERVER INFORMATION
    def bgrewriteaof(self):
        "Tell the Redis server to rewrite the AOF file from data in memory."
        return self.execute_command('BGREWRITEAOF')

    def bgsave(self):
        """
        Tell the Redis server to save its data to disk.  Unlike save(),
        this method is asynchronous and returns immediately.
        """
        return self.execute_command('BGSAVE')

    def client_kill(self, address):
        "Disconnects the client at ``address`` (ip:port)"
        return self.execute_command('CLIENT KILL', address)

    def client_kill_filter(self, _id=None, _type=None, addr=None, skipme=None):
        """
        Disconnects client(s) using a variety of filter options
        :param id: Kills a client by its unique ID field
        :param type: Kills a client by type where type is one of 'normal',
        'master', 'slave' or 'pubsub'
        :param addr: Kills a client by its 'address:port'
        :param skipme: If True, then the client calling the command
        will not get killed even if it is identified by one of the filter
        options. If skipme is not provided, the server defaults to skipme=True
        """
        args = []
        if _type is not None:
            client_types = ('normal', 'master', 'slave', 'pubsub')
            if str(_type).lower() not in client_types:
                raise DataError("CLIENT KILL type must be one of %r" % (
                                client_types,))
            args.extend((Token.get_token('TYPE'), _type))
        if skipme is not None:
            if not isinstance(skipme, bool):
                raise DataError("CLIENT KILL skipme must be a bool")
            if skipme:
                args.extend((Token.get_token('SKIPME'),
                             Token.get_token('YES')))
            else:
                args.extend((Token.get_token('SKIPME'),
                             Token.get_token('NO')))
        if _id is not None:
            args.extend((Token.get_token('ID'), _id))
        if addr is not None:
            args.extend((Token.get_token('ADDR'), addr))
        if not args:
            raise DataError("CLIENT KILL <filter> <value> ... ... <filter> "
                            "<value> must specify at least one filter")
        return self.execute_command('CLIENT KILL', *args)

    def client_list(self, _type=None):
        """
        Returns a list of currently connected clients.
        If type of client specified, only that type will be returned.
        :param _type: optional. one of the client types (normal, master,
         replica, pubsub)
        """
        "Returns a list of currently connected clients"
        if _type is not None:
            client_types = ('normal', 'master', 'replica', 'pubsub')
            if str(_type).lower() not in client_types:
                raise DataError("CLIENT LIST _type must be one of %r" % (
                                client_types,))
            return self.execute_command('CLIENT LIST', Token.get_token('TYPE'),
                                        _type)
        return self.execute_command('CLIENT LIST')

    def client_getname(self):
        "Returns the current connection name"
        return self.execute_command('CLIENT GETNAME')

    def client_id(self):
        "Returns the current connection id"
        return self.execute_command('CLIENT ID')

    def client_setname(self, name):
        "Sets the current connection name"
        return self.execute_command('CLIENT SETNAME', name)

    def client_unblock(self, client_id, error=False):
        """
        Unblocks a connection by its client id.
        If ``error`` is True, unblocks the client with a special error message.
        If ``error`` is False (default), the client is unblocked using the
        regular timeout mechanism.
        """
        args = ['CLIENT UNBLOCK', int(client_id)]
        if error:
            args.append(Token.get_token('ERROR'))
        return self.execute_command(*args)

    def client_pause(self, timeout):
        """
        Suspend all the Redis clients for the specified amount of time
        :param timeout: milliseconds to pause clients
        """
        if not isinstance(timeout, (int, long)):
            raise DataError("CLIENT PAUSE timeout must be an integer")
        return self.execute_command('CLIENT PAUSE', str(timeout))

    def config_get(self, pattern="*"):
        "Return a dictionary of configuration based on the ``pattern``"
        return self.execute_command('CONFIG GET', pattern)

    def config_set(self, name, value):
        "Set config item ``name`` with ``value``"
        return self.execute_command('CONFIG SET', name, value)

    def config_resetstat(self):
        "Reset runtime statistics"
        return self.execute_command('CONFIG RESETSTAT')

    def config_rewrite(self):
        "Rewrite config file with the minimal change to reflect running config"
        return self.execute_command('CONFIG REWRITE')

    def dbsize(self):
        "Returns the number of keys in the current database"
        return self.execute_command('DBSIZE')

    def debug_object(self, key):
        "Returns version specific meta information about a given key"
        return self.execute_command('DEBUG OBJECT', key)

    def echo(self, value):
        "Echo the string back from the server"
        return self.execute_command('ECHO', value)

    def flushall(self, asynchronous=False):
        """
        Delete all keys in all databases on the current host.

        ``asynchronous`` indicates whether the operation is
        executed asynchronously by the server.
        """
        args = []
        if asynchronous:
            args.append(Token.get_token('ASYNC'))
        return self.execute_command('FLUSHALL', *args)

    def flushdb(self, asynchronous=False):
        """
        Delete all keys in the current database.

        ``asynchronous`` indicates whether the operation is
        executed asynchronously by the server.
        """
        args = []
        if asynchronous:
            args.append(Token.get_token('ASYNC'))
        return self.execute_command('FLUSHDB', *args)

    def swapdb(self, first, second):
        "Swap two databases"
        return self.execute_command('SWAPDB', first, second)

    def info(self, section=None):
        """
        Returns a dictionary containing information about the Redis server

        The ``section`` option can be used to select a specific section
        of information

        The section option is not supported by older versions of Redis Server,
        and will generate ResponseError
        """
        if section is None:
            return self.execute_command('INFO')
        else:
            return self.execute_command('INFO', section)

    def lastsave(self):
        """
        Return a Python datetime object representing the last time the
        Redis database was saved to disk
        """
        return self.execute_command('LASTSAVE')

    def migrate(self, host, port, keys, destination_db, timeout,
                copy=False, replace=False, auth=None):
        """
        Migrate 1 or more keys from the current Redis server to a different
        server specified by the ``host``, ``port`` and ``destination_db``.

        The ``timeout``, specified in milliseconds, indicates the maximum
        time the connection between the two servers can be idle before the
        command is interrupted.

        If ``copy`` is True, the specified ``keys`` are NOT deleted from
        the source server.

        If ``replace`` is True, this operation will overwrite the keys
        on the destination server if they exist.

        If ``auth`` is specified, authenticate to the destination server with
        the password provided.
        """
        keys = list_or_args(keys, [])
        if not keys:
            raise DataError('MIGRATE requires at least one key')
        pieces = []
        if copy:
            pieces.append(Token.get_token('COPY'))
        if replace:
            pieces.append(Token.get_token('REPLACE'))
        if auth:
            pieces.append(Token.get_token('AUTH'))
            pieces.append(auth)
        pieces.append(Token.get_token('KEYS'))
        pieces.extend(keys)
        return self.execute_command('MIGRATE', host, port, '', destination_db,
                                    timeout, *pieces)

    def object(self, infotype, key):
        "Return the encoding, idletime, or refcount about the key"
        return self.execute_command('OBJECT', infotype, key, infotype=infotype)

    def memory_usage(self, key, samples=None):
        """
        Return the total memory usage for key, its value and associated
        administrative overheads.

        For nested data structures, ``samples`` is the number of elements to
        sample. If left unspecified, the server's default is 5. Use 0 to sample
        all elements.
        """
        args = []
        if isinstance(samples, int):
            args.extend([Token.get_token('SAMPLES'), samples])
        return self.execute_command('MEMORY USAGE', key, *args)

    def memory_purge(self):
        "Attempts to purge dirty pages for reclamation by allocator"
        return self.execute_command('MEMORY PURGE')

    def ping(self):
        "Ping the Redis server"
        return self.execute_command('PING')

    def save(self):
        """
        Tell the Redis server to save its data to disk,
        blocking until the save is complete
        """
        return self.execute_command('SAVE')

    def sentinel(self, *args):
        "Redis Sentinel's SENTINEL command."
        warnings.warn(
            DeprecationWarning('Use the individual sentinel_* methods'))

    def sentinel_get_master_addr_by_name(self, service_name):
        "Returns a (host, port) pair for the given ``service_name``"
        return self.execute_command('SENTINEL GET-MASTER-ADDR-BY-NAME',
                                    service_name)

    def sentinel_master(self, service_name):
        "Returns a dictionary containing the specified masters state."
        return self.execute_command('SENTINEL MASTER', service_name)

    def sentinel_masters(self):
        "Returns a list of dictionaries containing each master's state."
        return self.execute_command('SENTINEL MASTERS')

    def sentinel_monitor(self, name, ip, port, quorum):
        "Add a new master to Sentinel to be monitored"
        return self.execute_command('SENTINEL MONITOR', name, ip, port, quorum)

    def sentinel_remove(self, name):
        "Remove a master from Sentinel's monitoring"
        return self.execute_command('SENTINEL REMOVE', name)

    def sentinel_sentinels(self, service_name):
        "Returns a list of sentinels for ``service_name``"
        return self.execute_command('SENTINEL SENTINELS', service_name)

    def sentinel_set(self, name, option, value):
        "Set Sentinel monitoring parameters for a given master"
        return self.execute_command('SENTINEL SET', name, option, value)

    def sentinel_slaves(self, service_name):
        "Returns a list of slaves for ``service_name``"
        return self.execute_command('SENTINEL SLAVES', service_name)

    def shutdown(self, save=False, nosave=False):
        """Shutdown the Redis server.  If Redis has persistence configured,
        data will be flushed before shutdown.  If the "save" option is set,
        a data flush will be attempted even if there is no persistence
        configured.  If the "nosave" option is set, no data flush will be
        attempted.  The "save" and "nosave" options cannot both be set.
        """
        if save and nosave:
            raise DataError('SHUTDOWN save and nosave cannot both be set')
        args = ['SHUTDOWN']
        if save:
            args.append('SAVE')
        if nosave:
            args.append('NOSAVE')
        try:
            self.execute_command(*args)
        except ConnectionError:
            # a ConnectionError here is expected
            return
        raise RedisError("SHUTDOWN seems to have failed.")

    def slaveof(self, host=None, port=None):
        """
        Set the server to be a replicated slave of the instance identified
        by the ``host`` and ``port``. If called without arguments, the
        instance is promoted to a master instead.
        """
        if host is None and port is None:
            return self.execute_command('SLAVEOF', Token.get_token('NO'),
                                        Token.get_token('ONE'))
        return self.execute_command('SLAVEOF', host, port)

    def slowlog_get(self, num=None):
        """
        Get the entries from the slowlog. If ``num`` is specified, get the
        most recent ``num`` items.
        """
        args = ['SLOWLOG GET']
        if num is not None:
            args.append(num)
        return self.execute_command(*args)

    def slowlog_len(self):
        "Get the number of items in the slowlog"
        return self.execute_command('SLOWLOG LEN')

    def slowlog_reset(self):
        "Remove all items in the slowlog"
        return self.execute_command('SLOWLOG RESET')

    def time(self):
        """
        Returns the server time as a 2-item tuple of ints:
        (seconds since epoch, microseconds into this second).
        """
        return self.execute_command('TIME')

    def wait(self, num_replicas, timeout):
        """
        Redis synchronous replication
        That returns the number of replicas that processed the query when
        we finally have at least ``num_replicas``, or when the ``timeout`` was
        reached.
        """
        return self.execute_command('WAIT', num_replicas, timeout)

    # BASIC KEY COMMANDS
    def append(self, key, value):
        """
        Appends the string ``value`` to the value at ``key``. If ``key``
        doesn't already exist, create it with a value of ``value``.
        Returns the new length of the value at ``key``.
        """
        return self.execute_command('APPEND', key, value)

    def bitcount(self, key, start=None, end=None):
        """
        Returns the count of set bits in the value of ``key``.  Optional
        ``start`` and ``end`` paramaters indicate which bytes to consider
        """
        params = [key]
        if start is not None and end is not None:
            params.append(start)
            params.append(end)
        elif (start is not None and end is None) or \
                (end is not None and start is None):
            raise DataError("Both start and end must be specified")
        return self.execute_command('BITCOUNT', *params)

    def bitfield(self, key, default_overflow=None):
        """
        Return a BitFieldOperation instance to conveniently construct one or
        more bitfield operations on ``key``.
        """
        return BitFieldOperation(self, key, default_overflow=default_overflow)

    def bitop(self, operation, dest, *keys):
        """
        Perform a bitwise operation using ``operation`` between ``keys`` and
        store the result in ``dest``.
        """
        return self.execute_command('BITOP', operation, dest, *keys)

    def bitpos(self, key, bit, start=None, end=None):
        """
        Return the position of the first bit set to 1 or 0 in a string.
        ``start`` and ``end`` difines search range. The range is interpreted
        as a range of bytes and not a range of bits, so start=0 and end=2
        means to look at the first three bytes.
        """
        if bit not in (0, 1):
            raise DataError('bit must be 0 or 1')
        params = [key, bit]

        start is not None and params.append(start)

        if start is not None and end is not None:
            params.append(end)
        elif start is None and end is not None:
            raise DataError("start argument is not set, "
                            "when end is specified")
        return self.execute_command('BITPOS', *params)

    def decr(self, name, amount=1):
        """
        Decrements the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as 0 - ``amount``
        """
        # An alias for ``decr()``, because it is already implemented
        # as DECRBY redis command.
        return self.decrby(name, amount)

    def decrby(self, name, amount=1):
        """
        Decrements the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as 0 - ``amount``
        """
        return self.execute_command('DECRBY', name, amount)

    def delete(self, *names):
        "Delete one or more keys specified by ``names``"
        return self.execute_command('DEL', *names)

    def __delitem__(self, name):
        self.delete(name)

    def dump(self, name):
        """
        Return a serialized version of the value stored at the specified key.
        If key does not exist a nil bulk reply is returned.
        """
        return self.execute_command('DUMP', name)

    def exists(self, *names):
        "Returns the number of ``names`` that exist"
        return self.execute_command('EXISTS', *names)
    __contains__ = exists

    def expire(self, name, time):
        """
        Set an expire flag on key ``name`` for ``time`` seconds. ``time``
        can be represented by an integer or a Python timedelta object.
        """
        if isinstance(time, datetime.timedelta):
            time = int(time.total_seconds())
        return self.execute_command('EXPIRE', name, time)

    def expireat(self, name, when):
        """
        Set an expire flag on key ``name``. ``when`` can be represented
        as an integer indicating unix time or a Python datetime object.
        """
        if isinstance(when, datetime.datetime):
            when = int(mod_time.mktime(when.timetuple()))
        return self.execute_command('EXPIREAT', name, when)

    def get(self, name):
        """
        Return the value at key ``name``, or None if the key doesn't exist
        """
        return self.execute_command('GET', name)

    def __getitem__(self, name):
        """
        Return the value at key ``name``, raises a KeyError if the key
        doesn't exist.
        """
        value = self.get(name)
        if value is not None:
            return value
        raise KeyError(name)

    def getbit(self, name, offset):
        "Returns a boolean indicating the value of ``offset`` in ``name``"
        return self.execute_command('GETBIT', name, offset)

    def getrange(self, key, start, end):
        """
        Returns the substring of the string value stored at ``key``,
        determined by the offsets ``start`` and ``end`` (both are inclusive)
        """
        return self.execute_command('GETRANGE', key, start, end)

    def getset(self, name, value):
        """
        Sets the value at key ``name`` to ``value``
        and returns the old value at key ``name`` atomically.
        """
        return self.execute_command('GETSET', name, value)

    def incr(self, name, amount=1):
        """
        Increments the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as ``amount``
        """
        return self.incrby(name, amount)

    def incrby(self, name, amount=1):
        """
        Increments the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as ``amount``
        """
        # An alias for ``incr()``, because it is already implemented
        # as INCRBY redis command.
        return self.execute_command('INCRBY', name, amount)

    def incrbyfloat(self, name, amount=1.0):
        """
        Increments the value at key ``name`` by floating ``amount``.
        If no key exists, the value will be initialized as ``amount``
        """
        return self.execute_command('INCRBYFLOAT', name, amount)

    def keys(self, pattern='*'):
        "Returns a list of keys matching ``pattern``"
        return self.execute_command('KEYS', pattern)

    def mget(self, keys, *args):
        """
        Returns a list of values ordered identically to ``keys``
        """
        args = list_or_args(keys, args)
        options = {}
        if not args:
            options[EMPTY_RESPONSE] = []
        return self.execute_command('MGET', *args, **options)

    def mset(self, mapping):
        """
        Sets key/values based on a mapping. Mapping is a dictionary of
        key/value pairs. Both keys and values should be strings or types that
        can be cast to a string via str().
        """
        items = []
        for pair in iteritems(mapping):
            items.extend(pair)
        return self.execute_command('MSET', *items)

    def msetnx(self, mapping):
        """
        Sets key/values based on a mapping if none of the keys are already set.
        Mapping is a dictionary of key/value pairs. Both keys and values
        should be strings or types that can be cast to a string via str().
        Returns a boolean indicating if the operation was successful.
        """
        items = []
        for pair in iteritems(mapping):
            items.extend(pair)
        return self.execute_command('MSETNX', *items)

    def move(self, name, db):
        "Moves the key ``name`` to a different Redis database ``db``"
        return self.execute_command('MOVE', name, db)

    def persist(self, name):
        "Removes an expiration on ``name``"
        return self.execute_command('PERSIST', name)

    def pexpire(self, name, time):
        """
        Set an expire flag on key ``name`` for ``time`` milliseconds.
        ``time`` can be represented by an integer or a Python timedelta
        object.
        """
        if isinstance(time, datetime.timedelta):
            time = int(time.total_seconds() * 1000)
        return self.execute_command('PEXPIRE', name, time)

    def pexpireat(self, name, when):
        """
        Set an expire flag on key ``name``. ``when`` can be represented
        as an integer representing unix time in milliseconds (unix time * 1000)
        or a Python datetime object.
        """
        if isinstance(when, datetime.datetime):
            ms = int(when.microsecond / 1000)
            when = int(mod_time.mktime(when.timetuple())) * 1000 + ms
        return self.execute_command('PEXPIREAT', name, when)

    def psetex(self, name, time_ms, value):
        """
        Set the value of key ``name`` to ``value`` that expires in ``time_ms``
        milliseconds. ``time_ms`` can be represented by an integer or a Python
        timedelta object
        """
        if isinstance(time_ms, datetime.timedelta):
            time_ms = int(time_ms.total_seconds() * 1000)
        return self.execute_command('PSETEX', name, time_ms, value)

    def pttl(self, name):
        "Returns the number of milliseconds until the key ``name`` will expire"
        return self.execute_command('PTTL', name)

    def randomkey(self):
        "Returns the name of a random key"
        return self.execute_command('RANDOMKEY')

    def rename(self, src, dst):
        """
        Rename key ``src`` to ``dst``
        """
        return self.execute_command('RENAME', src, dst)

    def renamenx(self, src, dst):
        "Rename key ``src`` to ``dst`` if ``dst`` doesn't already exist"
        return self.execute_command('RENAMENX', src, dst)

    def restore(self, name, ttl, value, replace=False):
        """
        Create a key using the provided serialized value, previously obtained
        using DUMP.
        """
        params = [name, ttl, value]
        if replace:
            params.append('REPLACE')
        return self.execute_command('RESTORE', *params)

    def set(self, name, value, ex=None, px=None, nx=False, xx=False):
        """
        Set the value at key ``name`` to ``value``

        ``ex`` sets an expire flag on key ``name`` for ``ex`` seconds.

        ``px`` sets an expire flag on key ``name`` for ``px`` milliseconds.

        ``nx`` if set to True, set the value at key ``name`` to ``value`` only
            if it does not exist.

        ``xx`` if set to True, set the value at key ``name`` to ``value`` only
            if it already exists.
        """
        pieces = [name, value]
        if ex is not None:
            pieces.append('EX')
            if isinstance(ex, datetime.timedelta):
                ex = int(ex.total_seconds())
            pieces.append(ex)
        if px is not None:
            pieces.append('PX')
            if isinstance(px, datetime.timedelta):
                px = int(px.total_seconds() * 1000)
            pieces.append(px)

        if nx:
            pieces.append('NX')
        if xx:
            pieces.append('XX')
        return self.execute_command('SET', *pieces)

    def __setitem__(self, name, value):
        self.set(name, value)

    def setbit(self, name, offset, value):
        """
        Flag the ``offset`` in ``name`` as ``value``. Returns a boolean
        indicating the previous value of ``offset``.
        """
        value = value and 1 or 0
        return self.execute_command('SETBIT', name, offset, value)

    def setex(self, name, time, value):
        """
        Set the value of key ``name`` to ``value`` that expires in ``time``
        seconds. ``time`` can be represented by an integer or a Python
        timedelta object.
        """
        if isinstance(time, datetime.timedelta):
            time = int(time.total_seconds())
        return self.execute_command('SETEX', name, time, value)

    def setnx(self, name, value):
        "Set the value of key ``name`` to ``value`` if key doesn't exist"
        return self.execute_command('SETNX', name, value)

    def setrange(self, name, offset, value):
        """
        Overwrite bytes in the value of ``name`` starting at ``offset`` with
        ``value``. If ``offset`` plus the length of ``value`` exceeds the
        length of the original value, the new value will be larger than before.
        If ``offset`` exceeds the length of the original value, null bytes
        will be used to pad between the end of the previous value and the start
        of what's being injected.

        Returns the length of the new string.
        """
        return self.execute_command('SETRANGE', name, offset, value)

    def strlen(self, name):
        "Return the number of bytes stored in the value of ``name``"
        return self.execute_command('STRLEN', name)

    def substr(self, name, start, end=-1):
        """
        Return a substring of the string at key ``name``. ``start`` and ``end``
        are 0-based integers specifying the portion of the string to return.
        """
        return self.execute_command('SUBSTR', name, start, end)

    def touch(self, *args):
        """
        Alters the last access time of a key(s) ``*args``. A key is ignored
        if it does not exist.
        """
        return self.execute_command('TOUCH', *args)

    def ttl(self, name):
        "Returns the number of seconds until the key ``name`` will expire"
        return self.execute_command('TTL', name)

    def type(self, name):
        "Returns the type of key ``name``"
        return self.execute_command('TYPE', name)

    def watch(self, *names):
        """
        Watches the values at keys ``names``, or None if the key doesn't exist
        """
        warnings.warn(DeprecationWarning('Call WATCH from a Pipeline object'))

    def unwatch(self):
        """
        Unwatches the value at key ``name``, or None of the key doesn't exist
        """
        warnings.warn(
            DeprecationWarning('Call UNWATCH from a Pipeline object'))

    def unlink(self, *names):
        "Unlink one or more keys specified by ``names``"
        return self.execute_command('UNLINK', *names)

    # LIST COMMANDS
    def blpop(self, keys, timeout=0):
        """
        LPOP a value off of the first non-empty list
        named in the ``keys`` list.

        If none of the lists in ``keys`` has a value to LPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.

        If timeout is 0, then block indefinitely.
        """
        if timeout is None:
            timeout = 0
        keys = list_or_args(keys, None)
        keys.append(timeout)
        return self.execute_command('BLPOP', *keys)

    def brpop(self, keys, timeout=0):
        """
        RPOP a value off of the first non-empty list
        named in the ``keys`` list.

        If none of the lists in ``keys`` has a value to RPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.

        If timeout is 0, then block indefinitely.
        """
        if timeout is None:
            timeout = 0
        keys = list_or_args(keys, None)
        keys.append(timeout)
        return self.execute_command('BRPOP', *keys)

    def brpoplpush(self, src, dst, timeout=0):
        """
        Pop a value off the tail of ``src``, push it on the head of ``dst``
        and then return it.

        This command blocks until a value is in ``src`` or until ``timeout``
        seconds elapse, whichever is first. A ``timeout`` value of 0 blocks
        forever.
        """
        if timeout is None:
            timeout = 0
        return self.execute_command('BRPOPLPUSH', src, dst, timeout)

    def lindex(self, name, index):
        """
        Return the item from list ``name`` at position ``index``

        Negative indexes are supported and will return an item at the
        end of the list
        """
        return self.execute_command('LINDEX', name, index)

    def linsert(self, name, where, refvalue, value):
        """
        Insert ``value`` in list ``name`` either immediately before or after
        [``where``] ``refvalue``

        Returns the new length of the list on success or -1 if ``refvalue``
        is not in the list.
        """
        return self.execute_command('LINSERT', name, where, refvalue, value)

    def llen(self, name):
        "Return the length of the list ``name``"
        return self.execute_command('LLEN', name)

    def lpop(self, name):
        "Remove and return the first item of the list ``name``"
        return self.execute_command('LPOP', name)

    def lpush(self, name, *values):
        "Push ``values`` onto the head of the list ``name``"
        return self.execute_command('LPUSH', name, *values)

    def lpushx(self, name, value):
        "Push ``value`` onto the head of the list ``name`` if ``name`` exists"
        return self.execute_command('LPUSHX', name, value)

    def lrange(self, name, start, end):
        """
        Return a slice of the list ``name`` between
        position ``start`` and ``end``

        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation
        """
        return self.execute_command('LRANGE', name, start, end)

    def lrem(self, name, count, value):
        """
        Remove the first ``count`` occurrences of elements equal to ``value``
        from the list stored at ``name``.

        The count argument influences the operation in the following ways:
            count > 0: Remove elements equal to value moving from head to tail.
            count < 0: Remove elements equal to value moving from tail to head.
            count = 0: Remove all elements equal to value.
        """
        return self.execute_command('LREM', name, count, value)

    def lset(self, name, index, value):
        "Set ``position`` of list ``name`` to ``value``"
        return self.execute_command('LSET', name, index, value)

    def ltrim(self, name, start, end):
        """
        Trim the list ``name``, removing all values not within the slice
        between ``start`` and ``end``

        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation
        """
        return self.execute_command('LTRIM', name, start, end)

    def rpop(self, name):
        "Remove and return the last item of the list ``name``"
        return self.execute_command('RPOP', name)

    def rpoplpush(self, src, dst):
        """
        RPOP a value off of the ``src`` list and atomically LPUSH it
        on to the ``dst`` list.  Returns the value.
        """
        return self.execute_command('RPOPLPUSH', src, dst)

    def rpush(self, name, *values):
        "Push ``values`` onto the tail of the list ``name``"
        return self.execute_command('RPUSH', name, *values)

    def rpushx(self, name, value):
        "Push ``value`` onto the tail of the list ``name`` if ``name`` exists"
        return self.execute_command('RPUSHX', name, value)

    def sort(self, name, start=None, num=None, by=None, get=None,
             desc=False, alpha=False, store=None, groups=False):
        """
        Sort and return the list, set or sorted set at ``name``.

        ``start`` and ``num`` allow for paging through the sorted data

        ``by`` allows using an external key to weight and sort the items.
            Use an "*" to indicate where in the key the item value is located

        ``get`` allows for returning items from external keys rather than the
            sorted data itself.  Use an "*" to indicate where int he key
            the item value is located

        ``desc`` allows for reversing the sort

        ``alpha`` allows for sorting lexicographically rather than numerically

        ``store`` allows for storing the result of the sort into
            the key ``store``

        ``groups`` if set to True and if ``get`` contains at least two
            elements, sort will return a list of tuples, each containing the
            values fetched from the arguments to ``get``.

        """
        if (start is not None and num is None) or \
                (num is not None and start is None):
            raise DataError("``start`` and ``num`` must both be specified")

        pieces = [name]
        if by is not None:
            pieces.append(Token.get_token('BY'))
            pieces.append(by)
        if start is not None and num is not None:
            pieces.append(Token.get_token('LIMIT'))
            pieces.append(start)
            pieces.append(num)
        if get is not None:
            # If get is a string assume we want to get a single value.
            # Otherwise assume it's an interable and we want to get multiple
            # values. We can't just iterate blindly because strings are
            # iterable.
            if isinstance(get, (bytes, basestring)):
                pieces.append(Token.get_token('GET'))
                pieces.append(get)
            else:
                for g in get:
                    pieces.append(Token.get_token('GET'))
                    pieces.append(g)
        if desc:
            pieces.append(Token.get_token('DESC'))
        if alpha:
            pieces.append(Token.get_token('ALPHA'))
        if store is not None:
            pieces.append(Token.get_token('STORE'))
            pieces.append(store)

        if groups:
            if not get or isinstance(get, (bytes, basestring)) or len(get) < 2:
                raise DataError('when using "groups" the "get" argument '
                                'must be specified and contain at least '
                                'two keys')

        options = {'groups': len(get) if groups else None}
        return self.execute_command('SORT', *pieces, **options)

    # SCAN COMMANDS
    def scan(self, cursor=0, match=None, count=None):
        """
        Incrementally return lists of key names. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        pieces = [cursor]
        if match is not None:
            pieces.extend([Token.get_token('MATCH'), match])
        if count is not None:
            pieces.extend([Token.get_token('COUNT'), count])
        return self.execute_command('SCAN', *pieces)

    def scan_iter(self, match=None, count=None):
        """
        Make an iterator using the SCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        cursor = '0'
        while cursor != 0:
            cursor, data = self.scan(cursor=cursor, match=match, count=count)
            for item in data:
                yield item

    def sscan(self, name, cursor=0, match=None, count=None):
        """
        Incrementally return lists of elements in a set. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        pieces = [name, cursor]
        if match is not None:
            pieces.extend([Token.get_token('MATCH'), match])
        if count is not None:
            pieces.extend([Token.get_token('COUNT'), count])
        return self.execute_command('SSCAN', *pieces)

    def sscan_iter(self, name, match=None, count=None):
        """
        Make an iterator using the SSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        cursor = '0'
        while cursor != 0:
            cursor, data = self.sscan(name, cursor=cursor,
                                      match=match, count=count)
            for item in data:
                yield item

    def hscan(self, name, cursor=0, match=None, count=None):
        """
        Incrementally return key/value slices in a hash. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        pieces = [name, cursor]
        if match is not None:
            pieces.extend([Token.get_token('MATCH'), match])
        if count is not None:
            pieces.extend([Token.get_token('COUNT'), count])
        return self.execute_command('HSCAN', *pieces)

    def hscan_iter(self, name, match=None, count=None):
        """
        Make an iterator using the HSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        cursor = '0'
        while cursor != 0:
            cursor, data = self.hscan(name, cursor=cursor,
                                      match=match, count=count)
            for item in data.items():
                yield item

    def zscan(self, name, cursor=0, match=None, count=None,
              score_cast_func=float):
        """
        Incrementally return lists of elements in a sorted set. Also return a
        cursor indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        ``score_cast_func`` a callable used to cast the score return value
        """
        pieces = [name, cursor]
        if match is not None:
            pieces.extend([Token.get_token('MATCH'), match])
        if count is not None:
            pieces.extend([Token.get_token('COUNT'), count])
        options = {'score_cast_func': score_cast_func}
        return self.execute_command('ZSCAN', *pieces, **options)

    def zscan_iter(self, name, match=None, count=None,
                   score_cast_func=float):
        """
        Make an iterator using the ZSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        ``score_cast_func`` a callable used to cast the score return value
        """
        cursor = '0'
        while cursor != 0:
            cursor, data = self.zscan(name, cursor=cursor, match=match,
                                      count=count,
                                      score_cast_func=score_cast_func)
            for item in data:
                yield item

    # SET COMMANDS
    def sadd(self, name, *values):
        "Add ``value(s)`` to set ``name``"
        return self.execute_command('SADD', name, *values)

    def scard(self, name):
        "Return the number of elements in set ``name``"
        return self.execute_command('SCARD', name)

    def sdiff(self, keys, *args):
        "Return the difference of sets specified by ``keys``"
        args = list_or_args(keys, args)
        return self.execute_command('SDIFF', *args)

    def sdiffstore(self, dest, keys, *args):
        """
        Store the difference of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        args = list_or_args(keys, args)
        return self.execute_command('SDIFFSTORE', dest, *args)

    def sinter(self, keys, *args):
        "Return the intersection of sets specified by ``keys``"
        args = list_or_args(keys, args)
        return self.execute_command('SINTER', *args)

    def sinterstore(self, dest, keys, *args):
        """
        Store the intersection of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        args = list_or_args(keys, args)
        return self.execute_command('SINTERSTORE', dest, *args)

    def sismember(self, name, value):
        "Return a boolean indicating if ``value`` is a member of set ``name``"
        return self.execute_command('SISMEMBER', name, value)

    def smembers(self, name):
        "Return all members of the set ``name``"
        return self.execute_command('SMEMBERS', name)

    def smove(self, src, dst, value):
        "Move ``value`` from set ``src`` to set ``dst`` atomically"
        return self.execute_command('SMOVE', src, dst, value)

    def spop(self, name, count=None):
        "Remove and return a random member of set ``name``"
        args = (count is not None) and [count] or []
        return self.execute_command('SPOP', name, *args)

    def srandmember(self, name, number=None):
        """
        If ``number`` is None, returns a random member of set ``name``.

        If ``number`` is supplied, returns a list of ``number`` random
        memebers of set ``name``. Note this is only available when running
        Redis 2.6+.
        """
        args = (number is not None) and [number] or []
        return self.execute_command('SRANDMEMBER', name, *args)

    def srem(self, name, *values):
        "Remove ``values`` from set ``name``"
        return self.execute_command('SREM', name, *values)

    def sunion(self, keys, *args):
        "Return the union of sets specified by ``keys``"
        args = list_or_args(keys, args)
        return self.execute_command('SUNION', *args)

    def sunionstore(self, dest, keys, *args):
        """
        Store the union of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        args = list_or_args(keys, args)
        return self.execute_command('SUNIONSTORE', dest, *args)

    # STREAMS COMMANDS
    def xack(self, name, groupname, *ids):
        """
        Acknowledges the successful processing of one or more messages.
        name: name of the stream.
        groupname: name of the consumer group.
        *ids: message ids to acknowlege.
        """
        return self.execute_command('XACK', name, groupname, *ids)

    def xadd(self, name, fields, id='*', maxlen=None, approximate=True):
        """
        Add to a stream.
        name: name of the stream
        fields: dict of field/value pairs to insert into the stream
        id: Location to insert this record. By default it is appended.
        maxlen: truncate old stream members beyond this size
        approximate: actual stream length may be slightly more than maxlen

        """
        pieces = []
        if maxlen is not None:
            if not isinstance(maxlen, (int, long)) or maxlen < 1:
                raise DataError('XADD maxlen must be a positive integer')
            pieces.append(Token.get_token('MAXLEN'))
            if approximate:
                pieces.append(Token.get_token('~'))
            pieces.append(str(maxlen))
        pieces.append(id)
        if not isinstance(fields, dict) or len(fields) == 0:
            raise DataError('XADD fields must be a non-empty dict')
        for pair in iteritems(fields):
            pieces.extend(pair)
        return self.execute_command('XADD', name, *pieces)

    def xclaim(self, name, groupname, consumername, min_idle_time, message_ids,
               idle=None, time=None, retrycount=None, force=False,
               justid=False):
        """
        Changes the ownership of a pending message.
        name: name of the stream.
        groupname: name of the consumer group.
        consumername: name of a consumer that claims the message.
        min_idle_time: filter messages that were idle less than this amount of
        milliseconds
        message_ids: non-empty list or tuple of message IDs to claim
        idle: optional. Set the idle time (last time it was delivered) of the
         message in ms
        time: optional integer. This is the same as idle but instead of a
         relative amount of milliseconds, it sets the idle time to a specific
         Unix time (in milliseconds).
        retrycount: optional integer. set the retry counter to the specified
         value. This counter is incremented every time a message is delivered
         again.
        force: optional boolean, false by default. Creates the pending message
         entry in the PEL even if certain specified IDs are not already in the
         PEL assigned to a different client.
        justid: optional boolean, false by default. Return just an array of IDs
         of messages successfully claimed, without returning the actual message
        """
        if not isinstance(min_idle_time, (int, long)) or min_idle_time < 0:
            raise DataError("XCLAIM min_idle_time must be a non negative "
                            "integer")
        if not isinstance(message_ids, (list, tuple)) or not message_ids:
            raise DataError("XCLAIM message_ids must be a non empty list or "
                            "tuple of message IDs to claim")

        kwargs = {}
        pieces = [name, groupname, consumername, str(min_idle_time)]
        pieces.extend(list(message_ids))

        if idle is not None:
            if not isinstance(idle, (int, long)):
                raise DataError("XCLAIM idle must be an integer")
            pieces.extend((Token.get_token('IDLE'), str(idle)))
        if time is not None:
            if not isinstance(time, (int, long)):
                raise DataError("XCLAIM time must be an integer")
            pieces.extend((Token.get_token('TIME'), str(time)))
        if retrycount is not None:
            if not isinstance(retrycount, (int, long)):
                raise DataError("XCLAIM retrycount must be an integer")
            pieces.extend((Token.get_token('RETRYCOUNT'), str(retrycount)))

        if force:
            if not isinstance(force, bool):
                raise DataError("XCLAIM force must be a boolean")
            pieces.append(Token.get_token('FORCE'))
        if justid:
            if not isinstance(justid, bool):
                raise DataError("XCLAIM justid must be a boolean")
            pieces.append(Token.get_token('JUSTID'))
            kwargs['parse_justid'] = True
        return self.execute_command('XCLAIM', *pieces, **kwargs)

    def xdel(self, name, *ids):
        """
        Deletes one or more messages from a stream.
        name: name of the stream.
        *ids: message ids to delete.
        """
        return self.execute_command('XDEL', name, *ids)

    def xgroup_create(self, name, groupname, id='$', mkstream=False):
        """
        Create a new consumer group associated with a stream.
        name: name of the stream.
        groupname: name of the consumer group.
        id: ID of the last item in the stream to consider already delivered.
        """
        pieces = ['XGROUP CREATE', name, groupname, id]
        if mkstream:
            pieces.append(Token.get_token('MKSTREAM'))
        return self.execute_command(*pieces)

    def xgroup_delconsumer(self, name, groupname, consumername):
        """
        Remove a specific consumer from a consumer group.
        Returns the number of pending messages that the consumer had before it
        was deleted.
        name: name of the stream.
        groupname: name of the consumer group.
        consumername: name of consumer to delete
        """
        return self.execute_command('XGROUP DELCONSUMER', name, groupname,
                                    consumername)

    def xgroup_destroy(self, name, groupname):
        """
        Destroy a consumer group.
        name: name of the stream.
        groupname: name of the consumer group.
        """
        return self.execute_command('XGROUP DESTROY', name, groupname)

    def xgroup_setid(self, name, groupname, id):
        """
        Set the consumer group last delivered ID to something else.
        name: name of the stream.
        groupname: name of the consumer group.
        id: ID of the last item in the stream to consider already delivered.
        """
        return self.execute_command('XGROUP SETID', name, groupname, id)

    def xinfo_consumers(self, name, groupname):
        """
        Returns general information about the consumers in the group.
        name: name of the stream.
        groupname: name of the consumer group.
        """
        return self.execute_command('XINFO CONSUMERS', name, groupname)

    def xinfo_groups(self, name):
        """
        Returns general information about the consumer groups of the stream.
        name: name of the stream.
        """
        return self.execute_command('XINFO GROUPS', name)

    def xinfo_stream(self, name):
        """
        Returns general information about the stream.
        name: name of the stream.
        """
        return self.execute_command('XINFO STREAM', name)

    def xlen(self, name):
        """
        Returns the number of elements in a given stream.
        """
        return self.execute_command('XLEN', name)

    def xpending(self, name, groupname):
        """
        Returns information about pending messages of a group.
        name: name of the stream.
        groupname: name of the consumer group.
        """
        return self.execute_command('XPENDING', name, groupname)

    def xpending_range(self, name, groupname, min, max, count,
                       consumername=None):
        """
        Returns information about pending messages, in a range.
        name: name of the stream.
        groupname: name of the consumer group.
        min: minimum stream ID.
        max: maximum stream ID.
        count: number of messages to return
        consumername: name of a consumer to filter by (optional).
        """
        pieces = [name, groupname]
        if min is not None or max is not None or count is not None:
            if min is None or max is None or count is None:
                raise DataError("XPENDING must be provided with min, max "
                                "and count parameters, or none of them. ")
            if not isinstance(count, (int, long)) or count < -1:
                raise DataError("XPENDING count must be a integer >= -1")
            pieces.extend((min, max, str(count)))
        if consumername is not None:
            if min is None or max is None or count is None:
                raise DataError("if XPENDING is provided with consumername,"
                                " it must be provided with min, max and"
                                " count parameters")
            pieces.append(consumername)
        return self.execute_command('XPENDING', *pieces, parse_detail=True)

    def xrange(self, name, min='-', max='+', count=None):
        """
        Read stream values within an interval.
        name: name of the stream.
        start: first stream ID. defaults to '-',
               meaning the earliest available.
        finish: last stream ID. defaults to '+',
                meaning the latest available.
        count: if set, only return this many items, beginning with the
               earliest available.
        """
        pieces = [min, max]
        if count is not None:
            if not isinstance(count, (int, long)) or count < 1:
                raise DataError('XRANGE count must be a positive integer')
            pieces.append(Token.get_token('COUNT'))
            pieces.append(str(count))

        return self.execute_command('XRANGE', name, *pieces)

    def xread(self, streams, count=None, block=None):
        """
        Block and monitor multiple streams for new data.
        streams: a dict of stream names to stream IDs, where
                   IDs indicate the last ID already seen.
        count: if set, only return this many items, beginning with the
               earliest available.
        block: number of milliseconds to wait, if nothing already present.
        """
        pieces = []
        if block is not None:
            if not isinstance(block, (int, long)) or block < 0:
                raise DataError('XREAD block must be a non-negative integer')
            pieces.append(Token.get_token('BLOCK'))
            pieces.append(str(block))
        if count is not None:
            if not isinstance(count, (int, long)) or count < 1:
                raise DataError('XREAD count must be a positive integer')
            pieces.append(Token.get_token('COUNT'))
            pieces.append(str(count))
        if not isinstance(streams, dict) or len(streams) == 0:
            raise DataError('XREAD streams must be a non empty dict')
        pieces.append(Token.get_token('STREAMS'))
        keys, values = izip(*iteritems(streams))
        pieces.extend(keys)
        pieces.extend(values)
        return self.execute_command('XREAD', *pieces)

    def xreadgroup(self, groupname, consumername, streams, count=None,
                   block=None, noack=False):
        """
        Read from a stream via a consumer group.
        groupname: name of the consumer group.
        consumername: name of the requesting consumer.
        streams: a dict of stream names to stream IDs, where
               IDs indicate the last ID already seen.
        count: if set, only return this many items, beginning with the
               earliest available.
        block: number of milliseconds to wait, if nothing already present.
        noack: do not add messages to the PEL
        """
        pieces = [Token.get_token('GROUP'), groupname, consumername]
        if count is not None:
            if not isinstance(count, (int, long)) or count < 1:
                raise DataError("XREADGROUP count must be a positive integer")
            pieces.append(Token.get_token("COUNT"))
            pieces.append(str(count))
        if block is not None:
            if not isinstance(block, (int, long)) or block < 0:
                raise DataError("XREADGROUP block must be a non-negative "
                                "integer")
            pieces.append(Token.get_token("BLOCK"))
            pieces.append(str(block))
        if noack:
            pieces.append(Token.get_token("NOACK"))
        if not isinstance(streams, dict) or len(streams) == 0:
            raise DataError('XREADGROUP streams must be a non empty dict')
        pieces.append(Token.get_token('STREAMS'))
        pieces.extend(streams.keys())
        pieces.extend(streams.values())
        return self.execute_command('XREADGROUP', *pieces)

    def xrevrange(self, name, max='+', min='-', count=None):
        """
        Read stream values within an interval, in reverse order.
        name: name of the stream
        start: first stream ID. defaults to '+',
               meaning the latest available.
        finish: last stream ID. defaults to '-',
                meaning the earliest available.
        count: if set, only return this many items, beginning with the
               latest available.
        """
        pieces = [max, min]
        if count is not None:
            if not isinstance(count, (int, long)) or count < 1:
                raise DataError('XREVRANGE count must be a positive integer')
            pieces.append(Token.get_token('COUNT'))
            pieces.append(str(count))

        return self.execute_command('XREVRANGE', name, *pieces)

    def xtrim(self, name, maxlen, approximate=True):
        """
        Trims old messages from a stream.
        name: name of the stream.
        maxlen: truncate old stream messages beyond this size
        approximate: actual stream length may be slightly more than maxlen
        """
        pieces = [Token.get_token('MAXLEN')]
        if approximate:
            pieces.append(Token.get_token('~'))
        pieces.append(maxlen)
        return self.execute_command('XTRIM', name, *pieces)

    # SORTED SET COMMANDS
    def zadd(self, name, mapping, nx=False, xx=False, ch=False, incr=False):
        """
        Set any number of element-name, score pairs to the key ``name``. Pairs
        are specified as a dict of element-names keys to score values.

        ``nx`` forces ZADD to only create new elements and not to update
        scores for elements that already exist.

        ``xx`` forces ZADD to only update scores of elements that already
        exist. New elements will not be added.

        ``ch`` modifies the return value to be the numbers of elements changed.
        Changed elements include new elements that were added and elements
        whose scores changed.

        ``incr`` modifies ZADD to behave like ZINCRBY. In this mode only a
        single element/score pair can be specified and the score is the amount
        the existing score will be incremented by. When using this mode the
        return value of ZADD will be the new score of the element.

        The return value of ZADD varies based on the mode specified. With no
        options, ZADD returns the number of new elements added to the sorted
        set.
        """
        if not mapping:
            raise DataError("ZADD requires at least one element/score pair")
        if nx and xx:
            raise DataError("ZADD allows either 'nx' or 'xx', not both")
        if incr and len(mapping) != 1:
            raise DataError("ZADD option 'incr' only works when passing a "
                            "single element/score pair")
        pieces = []
        options = {}
        if nx:
            pieces.append(Token.get_token('NX'))
        if xx:
            pieces.append(Token.get_token('XX'))
        if ch:
            pieces.append(Token.get_token('CH'))
        if incr:
            pieces.append(Token.get_token('INCR'))
            options['as_score'] = True
        for pair in iteritems(mapping):
            pieces.append(pair[1])
            pieces.append(pair[0])
        return self.execute_command('ZADD', name, *pieces, **options)

    def zcard(self, name):
        "Return the number of elements in the sorted set ``name``"
        return self.execute_command('ZCARD', name)

    def zcount(self, name, min, max):
        """
        Returns the number of elements in the sorted set at key ``name`` with
        a score between ``min`` and ``max``.
        """
        return self.execute_command('ZCOUNT', name, min, max)

    def zincrby(self, name, amount, value):
        "Increment the score of ``value`` in sorted set ``name`` by ``amount``"
        return self.execute_command('ZINCRBY', name, amount, value)

    def zinterstore(self, dest, keys, aggregate=None):
        """
        Intersect multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.
        """
        return self._zaggregate('ZINTERSTORE', dest, keys, aggregate)

    def zlexcount(self, name, min, max):
        """
        Return the number of items in the sorted set ``name`` between the
        lexicographical range ``min`` and ``max``.
        """
        return self.execute_command('ZLEXCOUNT', name, min, max)

    def zpopmax(self, name, count=None):
        """
        Remove and return up to ``count`` members with the highest scores
        from the sorted set ``name``.
        """
        args = (count is not None) and [count] or []
        options = {
            'withscores': True
        }
        return self.execute_command('ZPOPMAX', name, *args, **options)

    def zpopmin(self, name, count=None):
        """
        Remove and return up to ``count`` members with the lowest scores
        from the sorted set ``name``.
        """
        args = (count is not None) and [count] or []
        options = {
            'withscores': True
        }
        return self.execute_command('ZPOPMIN', name, *args, **options)

    def bzpopmax(self, keys, timeout=0):
        """
        ZPOPMAX a value off of the first non-empty sorted set
        named in the ``keys`` list.

        If none of the sorted sets in ``keys`` has a value to ZPOPMAX,
        then block for ``timeout`` seconds, or until a member gets added
        to one of the sorted sets.

        If timeout is 0, then block indefinitely.
        """
        if timeout is None:
            timeout = 0
        keys = list_or_args(keys, None)
        keys.append(timeout)
        return self.execute_command('BZPOPMAX', *keys)

    def bzpopmin(self, keys, timeout=0):
        """
        ZPOPMIN a value off of the first non-empty sorted set
        named in the ``keys`` list.

        If none of the sorted sets in ``keys`` has a value to ZPOPMIN,
        then block for ``timeout`` seconds, or until a member gets added
        to one of the sorted sets.

        If timeout is 0, then block indefinitely.
        """
        if timeout is None:
            timeout = 0
        keys = list_or_args(keys, None)
        keys.append(timeout)
        return self.execute_command('BZPOPMIN', *keys)

    def zrange(self, name, start, end, desc=False, withscores=False,
               score_cast_func=float):
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in ascending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``desc`` a boolean indicating whether to sort the results descendingly

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value
        """
        if desc:
            return self.zrevrange(name, start, end, withscores,
                                  score_cast_func)
        pieces = ['ZRANGE', name, start, end]
        if withscores:
            pieces.append(Token.get_token('WITHSCORES'))
        options = {
            'withscores': withscores,
            'score_cast_func': score_cast_func
        }
        return self.execute_command(*pieces, **options)

    def zrangebylex(self, name, min, max, start=None, num=None):
        """
        Return the lexicographical range of values from sorted set ``name``
        between ``min`` and ``max``.

        If ``start`` and ``num`` are specified, then return a slice of the
        range.
        """
        if (start is not None and num is None) or \
                (num is not None and start is None):
            raise DataError("``start`` and ``num`` must both be specified")
        pieces = ['ZRANGEBYLEX', name, min, max]
        if start is not None and num is not None:
            pieces.extend([Token.get_token('LIMIT'), start, num])
        return self.execute_command(*pieces)

    def zrevrangebylex(self, name, max, min, start=None, num=None):
        """
        Return the reversed lexicographical range of values from sorted set
        ``name`` between ``max`` and ``min``.

        If ``start`` and ``num`` are specified, then return a slice of the
        range.
        """
        if (start is not None and num is None) or \
                (num is not None and start is None):
            raise DataError("``start`` and ``num`` must both be specified")
        pieces = ['ZREVRANGEBYLEX', name, max, min]
        if start is not None and num is not None:
            pieces.extend([Token.get_token('LIMIT'), start, num])
        return self.execute_command(*pieces)

    def zrangebyscore(self, name, min, max, start=None, num=None,
                      withscores=False, score_cast_func=float):
        """
        Return a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max``.

        If ``start`` and ``num`` are specified, then return a slice
        of the range.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        `score_cast_func`` a callable used to cast the score return value
        """
        if (start is not None and num is None) or \
                (num is not None and start is None):
            raise DataError("``start`` and ``num`` must both be specified")
        pieces = ['ZRANGEBYSCORE', name, min, max]
        if start is not None and num is not None:
            pieces.extend([Token.get_token('LIMIT'), start, num])
        if withscores:
            pieces.append(Token.get_token('WITHSCORES'))
        options = {
            'withscores': withscores,
            'score_cast_func': score_cast_func
        }
        return self.execute_command(*pieces, **options)

    def zrank(self, name, value):
        """
        Returns a 0-based value indicating the rank of ``value`` in sorted set
        ``name``
        """
        return self.execute_command('ZRANK', name, value)

    def zrem(self, name, *values):
        "Remove member ``values`` from sorted set ``name``"
        return self.execute_command('ZREM', name, *values)

    def zremrangebylex(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` between the
        lexicographical range specified by ``min`` and ``max``.

        Returns the number of elements removed.
        """
        return self.execute_command('ZREMRANGEBYLEX', name, min, max)

    def zremrangebyrank(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` with ranks between
        ``min`` and ``max``. Values are 0-based, ordered from smallest score
        to largest. Values can be negative indicating the highest scores.
        Returns the number of elements removed
        """
        return self.execute_command('ZREMRANGEBYRANK', name, min, max)

    def zremrangebyscore(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` with scores
        between ``min`` and ``max``. Returns the number of elements removed.
        """
        return self.execute_command('ZREMRANGEBYSCORE', name, min, max)

    def zrevrange(self, name, start, end, withscores=False,
                  score_cast_func=float):
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in descending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``withscores`` indicates to return the scores along with the values
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value
        """
        pieces = ['ZREVRANGE', name, start, end]
        if withscores:
            pieces.append(Token.get_token('WITHSCORES'))
        options = {
            'withscores': withscores,
            'score_cast_func': score_cast_func
        }
        return self.execute_command(*pieces, **options)

    def zrevrangebyscore(self, name, max, min, start=None, num=None,
                         withscores=False, score_cast_func=float):
        """
        Return a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max`` in descending order.

        If ``start`` and ``num`` are specified, then return a slice
        of the range.

        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs

        ``score_cast_func`` a callable used to cast the score return value
        """
        if (start is not None and num is None) or \
                (num is not None and start is None):
            raise DataError("``start`` and ``num`` must both be specified")
        pieces = ['ZREVRANGEBYSCORE', name, max, min]
        if start is not None and num is not None:
            pieces.extend([Token.get_token('LIMIT'), start, num])
        if withscores:
            pieces.append(Token.get_token('WITHSCORES'))
        options = {
            'withscores': withscores,
            'score_cast_func': score_cast_func
        }
        return self.execute_command(*pieces, **options)

    def zrevrank(self, name, value):
        """
        Returns a 0-based value indicating the descending rank of
        ``value`` in sorted set ``name``
        """
        return self.execute_command('ZREVRANK', name, value)

    def zscore(self, name, value):
        "Return the score of element ``value`` in sorted set ``name``"
        return self.execute_command('ZSCORE', name, value)

    def zunionstore(self, dest, keys, aggregate=None):
        """
        Union multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.
        """
        return self._zaggregate('ZUNIONSTORE', dest, keys, aggregate)

    def _zaggregate(self, command, dest, keys, aggregate=None):
        pieces = [command, dest, len(keys)]
        if isinstance(keys, dict):
            keys, weights = iterkeys(keys), itervalues(keys)
        else:
            weights = None
        pieces.extend(keys)
        if weights:
            pieces.append(Token.get_token('WEIGHTS'))
            pieces.extend(weights)
        if aggregate:
            pieces.append(Token.get_token('AGGREGATE'))
            pieces.append(aggregate)
        return self.execute_command(*pieces)

    # HYPERLOGLOG COMMANDS
    def pfadd(self, name, *values):
        "Adds the specified elements to the specified HyperLogLog."
        return self.execute_command('PFADD', name, *values)

    def pfcount(self, *sources):
        """
        Return the approximated cardinality of
        the set observed by the HyperLogLog at key(s).
        """
        return self.execute_command('PFCOUNT', *sources)

    def pfmerge(self, dest, *sources):
        "Merge N different HyperLogLogs into a single one."
        return self.execute_command('PFMERGE', dest, *sources)

    # HASH COMMANDS
    def hdel(self, name, *keys):
        "Delete ``keys`` from hash ``name``"
        return self.execute_command('HDEL', name, *keys)

    def hexists(self, name, key):
        "Returns a boolean indicating if ``key`` exists within hash ``name``"
        return self.execute_command('HEXISTS', name, key)

    def hget(self, name, key):
        "Return the value of ``key`` within the hash ``name``"
        return self.execute_command('HGET', name, key)

    def hgetall(self, name):
        "Return a Python dict of the hash's name/value pairs"
        return self.execute_command('HGETALL', name)

    def hincrby(self, name, key, amount=1):
        "Increment the value of ``key`` in hash ``name`` by ``amount``"
        return self.execute_command('HINCRBY', name, key, amount)

    def hincrbyfloat(self, name, key, amount=1.0):
        """
        Increment the value of ``key`` in hash ``name`` by floating ``amount``
        """
        return self.execute_command('HINCRBYFLOAT', name, key, amount)

    def hkeys(self, name):
        "Return the list of keys within hash ``name``"
        return self.execute_command('HKEYS', name)

    def hlen(self, name):
        "Return the number of elements in hash ``name``"
        return self.execute_command('HLEN', name)

    def hset(self, name, key, value):
        """
        Set ``key`` to ``value`` within hash ``name``
        Returns 1 if HSET created a new field, otherwise 0
        """
        return self.execute_command('HSET', name, key, value)

    def hsetnx(self, name, key, value):
        """
        Set ``key`` to ``value`` within hash ``name`` if ``key`` does not
        exist.  Returns 1 if HSETNX created a field, otherwise 0.
        """
        return self.execute_command('HSETNX', name, key, value)

    def hmset(self, name, mapping):
        """
        Set key to value within hash ``name`` for each corresponding
        key and value from the ``mapping`` dict.
        """
        if not mapping:
            raise DataError("'hmset' with 'mapping' of length 0")
        items = []
        for pair in iteritems(mapping):
            items.extend(pair)
        return self.execute_command('HMSET', name, *items)

    def hmget(self, name, keys, *args):
        "Returns a list of values ordered identically to ``keys``"
        args = list_or_args(keys, args)
        return self.execute_command('HMGET', name, *args)

    def hvals(self, name):
        "Return the list of values within hash ``name``"
        return self.execute_command('HVALS', name)

    def hstrlen(self, name, key):
        """
        Return the number of bytes stored in the value of ``key``
        within hash ``name``
        """
        return self.execute_command('HSTRLEN', name, key)

    def publish(self, channel, message):
        """
        Publish ``message`` on ``channel``.
        Returns the number of subscribers the message was delivered to.
        """
        return self.execute_command('PUBLISH', channel, message)

    def pubsub_channels(self, pattern='*'):
        """
        Return a list of channels that have at least one subscriber
        """
        return self.execute_command('PUBSUB CHANNELS', pattern)

    def pubsub_numpat(self):
        """
        Returns the number of subscriptions to patterns
        """
        return self.execute_command('PUBSUB NUMPAT')

    def pubsub_numsub(self, *args):
        """
        Return a list of (channel, number of subscribers) tuples
        for each channel given in ``*args``
        """
        return self.execute_command('PUBSUB NUMSUB', *args)

    def cluster(self, cluster_arg, *args):
        return self.execute_command('CLUSTER %s' % cluster_arg.upper(), *args)

    def eval(self, script, numkeys, *keys_and_args):
        """
        Execute the Lua ``script``, specifying the ``numkeys`` the script
        will touch and the key names and argument values in ``keys_and_args``.
        Returns the result of the script.

        In practice, use the object returned by ``register_script``. This
        function exists purely for Redis API completion.
        """
        return self.execute_command('EVAL', script, numkeys, *keys_and_args)

    def evalsha(self, sha, numkeys, *keys_and_args):
        """
        Use the ``sha`` to execute a Lua script already registered via EVAL
        or SCRIPT LOAD. Specify the ``numkeys`` the script will touch and the
        key names and argument values in ``keys_and_args``. Returns the result
        of the script.

        In practice, use the object returned by ``register_script``. This
        function exists purely for Redis API completion.
        """
        return self.execute_command('EVALSHA', sha, numkeys, *keys_and_args)

    def script_exists(self, *args):
        """
        Check if a script exists in the script cache by specifying the SHAs of
        each script as ``args``. Returns a list of boolean values indicating if
        if each already script exists in the cache.
        """
        return self.execute_command('SCRIPT EXISTS', *args)

    def script_flush(self):
        "Flush all scripts from the script cache"
        return self.execute_command('SCRIPT FLUSH')

    def script_kill(self):
        "Kill the currently executing Lua script"
        return self.execute_command('SCRIPT KILL')

    def script_load(self, script):
        "Load a Lua ``script`` into the script cache. Returns the SHA."
        return self.execute_command('SCRIPT LOAD', script)

    def register_script(self, script):
        """
        Register a Lua ``script`` specifying the ``keys`` it will touch.
        Returns a Script object that is callable and hides the complexity of
        deal with scripts, keys, and shas. This is the preferred way to work
        with Lua scripts.
        """
        return Script(self, script)

    # GEO COMMANDS
    def geoadd(self, name, *values):
        """
        Add the specified geospatial items to the specified key identified
        by the ``name`` argument. The Geospatial items are given as ordered
        members of the ``values`` argument, each item or place is formed by
        the triad longitude, latitude and name.
        """
        if len(values) % 3 != 0:
            raise DataError("GEOADD requires places with lon, lat and name"
                            " values")
        return self.execute_command('GEOADD', name, *values)

    def geodist(self, name, place1, place2, unit=None):
        """
        Return the distance between ``place1`` and ``place2`` members of the
        ``name`` key.
        The units must be one of the following : m, km mi, ft. By default
        meters are used.
        """
        pieces = [name, place1, place2]
        if unit and unit not in ('m', 'km', 'mi', 'ft'):
            raise DataError("GEODIST invalid unit")
        elif unit:
            pieces.append(unit)
        return self.execute_command('GEODIST', *pieces)

    def geohash(self, name, *values):
        """
        Return the geo hash string for each item of ``values`` members of
        the specified key identified by the ``name`` argument.
        """
        return self.execute_command('GEOHASH', name, *values)

    def geopos(self, name, *values):
        """
        Return the positions of each item of ``values`` as members of
        the specified key identified by the ``name`` argument. Each position
        is represented by the pairs lon and lat.
        """
        return self.execute_command('GEOPOS', name, *values)

    def georadius(self, name, longitude, latitude, radius, unit=None,
                  withdist=False, withcoord=False, withhash=False, count=None,
                  sort=None, store=None, store_dist=None):
        """
        Return the members of the specified key identified by the
        ``name`` argument which are within the borders of the area specified
        with the ``latitude`` and ``longitude`` location and the maximum
        distance from the center specified by the ``radius`` value.

        The units must be one of the following : m, km mi, ft. By default

        ``withdist`` indicates to return the distances of each place.

        ``withcoord`` indicates to return the latitude and longitude of
        each place.

        ``withhash`` indicates to return the geohash string of each place.

        ``count`` indicates to return the number of elements up to N.

        ``sort`` indicates to return the places in a sorted way, ASC for
        nearest to fairest and DESC for fairest to nearest.

        ``store`` indicates to save the places names in a sorted set named
        with a specific key, each element of the destination sorted set is
        populated with the score got from the original geo sorted set.

        ``store_dist`` indicates to save the places names in a sorted set
        named with a specific key, instead of ``store`` the sorted set
        destination score is set with the distance.
        """
        return self._georadiusgeneric('GEORADIUS',
                                      name, longitude, latitude, radius,
                                      unit=unit, withdist=withdist,
                                      withcoord=withcoord, withhash=withhash,
                                      count=count, sort=sort, store=store,
                                      store_dist=store_dist)

    def georadiusbymember(self, name, member, radius, unit=None,
                          withdist=False, withcoord=False, withhash=False,
                          count=None, sort=None, store=None, store_dist=None):
        """
        This command is exactly like ``georadius`` with the sole difference
        that instead of taking, as the center of the area to query, a longitude
        and latitude value, it takes the name of a member already existing
        inside the geospatial index represented by the sorted set.
        """
        return self._georadiusgeneric('GEORADIUSBYMEMBER',
                                      name, member, radius, unit=unit,
                                      withdist=withdist, withcoord=withcoord,
                                      withhash=withhash, count=count,
                                      sort=sort, store=store,
                                      store_dist=store_dist)

    def _georadiusgeneric(self, command, *args, **kwargs):
        pieces = list(args)
        if kwargs['unit'] and kwargs['unit'] not in ('m', 'km', 'mi', 'ft'):
            raise DataError("GEORADIUS invalid unit")
        elif kwargs['unit']:
            pieces.append(kwargs['unit'])
        else:
            pieces.append('m',)

        for token in ('withdist', 'withcoord', 'withhash'):
            if kwargs[token]:
                pieces.append(Token(token.upper()))

        if kwargs['count']:
            pieces.extend([Token('COUNT'), kwargs['count']])

        if kwargs['sort'] and kwargs['sort'] not in ('ASC', 'DESC'):
            raise DataError("GEORADIUS invalid sort")
        elif kwargs['sort']:
            pieces.append(Token(kwargs['sort']))

        if kwargs['store'] and kwargs['store_dist']:
            raise DataError("GEORADIUS store and store_dist cant be set"
                            " together")

        if kwargs['store']:
            pieces.extend([Token('STORE'), kwargs['store']])

        if kwargs['store_dist']:
            pieces.extend([Token('STOREDIST'), kwargs['store_dist']])

        return self.execute_command(command, *pieces, **kwargs)


StrictRedis = Redis


class PubSub(object):
    """
    PubSub provides publish, subscribe and listen support to Redis channels.

    After subscribing to one or more channels, the listen() method will block
    until a message arrives on one of the subscribed channels. That message
    will be returned and it's safe to start listening again.
    """
    PUBLISH_MESSAGE_TYPES = ('message', 'pmessage')
    UNSUBSCRIBE_MESSAGE_TYPES = ('unsubscribe', 'punsubscribe')

    def __init__(self, connection_pool, shard_hint=None,
                 ignore_subscribe_messages=False):
        self.connection_pool = connection_pool
        self.shard_hint = shard_hint
        self.ignore_subscribe_messages = ignore_subscribe_messages
        self.connection = None
        # we need to know the encoding options for this connection in order
        # to lookup channel and pattern names for callback handlers.
        self.encoder = self.connection_pool.get_encoder()
        self.reset()

    def __del__(self):
        try:
            # if this object went out of scope prior to shutting down
            # subscriptions, close the connection manually before
            # returning it to the connection pool
            self.reset()
        except Exception:
            pass

    def reset(self):
        if self.connection:
            self.connection.disconnect()
            self.connection.clear_connect_callbacks()
            self.connection_pool.release(self.connection)
            self.connection = None
        self.channels = {}
        self.pending_unsubscribe_channels = set()
        self.patterns = {}
        self.pending_unsubscribe_patterns = set()

    def close(self):
        self.reset()

    def on_connect(self, connection):
        "Re-subscribe to any channels and patterns previously subscribed to"
        # NOTE: for python3, we can't pass bytestrings as keyword arguments
        # so we need to decode channel/pattern names back to unicode strings
        # before passing them to [p]subscribe.
        self.pending_unsubscribe_channels.clear()
        self.pending_unsubscribe_patterns.clear()
        if self.channels:
            channels = {}
            for k, v in iteritems(self.channels):
                channels[self.encoder.decode(k, force=True)] = v
            self.subscribe(**channels)
        if self.patterns:
            patterns = {}
            for k, v in iteritems(self.patterns):
                patterns[self.encoder.decode(k, force=True)] = v
            self.psubscribe(**patterns)

    @property
    def subscribed(self):
        "Indicates if there are subscriptions to any channels or patterns"
        return bool(self.channels or self.patterns)

    def execute_command(self, *args, **kwargs):
        "Execute a publish/subscribe command"

        # NOTE: don't parse the response in this function -- it could pull a
        # legitimate message off the stack if the connection is already
        # subscribed to one or more channels

        if self.connection is None:
            self.connection = self.connection_pool.get_connection(
                'pubsub',
                self.shard_hint
            )
            # register a callback that re-subscribes to any channels we
            # were listening to when we were disconnected
            self.connection.register_connect_callback(self.on_connect)
        connection = self.connection
        self._execute(connection, connection.send_command, *args)

    def _execute(self, connection, command, *args):
        try:
            return command(*args)
        except (ConnectionError, TimeoutError) as e:
            connection.disconnect()
            if not (connection.retry_on_timeout and
                    isinstance(e, TimeoutError)):
                raise
            # Connect manually here. If the Redis server is down, this will
            # fail and raise a ConnectionError as desired.
            connection.connect()
            # the ``on_connect`` callback should haven been called by the
            # connection to resubscribe us to any channels and patterns we were
            # previously listening to
            return command(*args)

    def parse_response(self, block=True, timeout=0):
        "Parse the response from a publish/subscribe command"
        connection = self.connection
        if connection is None:
            raise RuntimeError(
                'pubsub connection not set: '
                'did you forget to call subscribe() or psubscribe()?')
        if not block and not connection.can_read(timeout=timeout):
            return None
        return self._execute(connection, connection.read_response)

    def _normalize_keys(self, data):
        """
        normalize channel/pattern names to be either bytes or strings
        based on whether responses are automatically decoded. this saves us
        from coercing the value for each message coming in.
        """
        encode = self.encoder.encode
        decode = self.encoder.decode
        return {decode(encode(k)): v for k, v in iteritems(data)}

    def psubscribe(self, *args, **kwargs):
        """
        Subscribe to channel patterns. Patterns supplied as keyword arguments
        expect a pattern name as the key and a callable as the value. A
        pattern's callable will be invoked automatically when a message is
        received on that pattern rather than producing a message via
        ``listen()``.
        """
        if args:
            args = list_or_args(args[0], args[1:])
        new_patterns = dict.fromkeys(args)
        new_patterns.update(kwargs)
        ret_val = self.execute_command('PSUBSCRIBE', *iterkeys(new_patterns))
        # update the patterns dict AFTER we send the command. we don't want to
        # subscribe twice to these patterns, once for the command and again
        # for the reconnection.
        new_patterns = self._normalize_keys(new_patterns)
        self.patterns.update(new_patterns)
        self.pending_unsubscribe_patterns.difference_update(new_patterns)
        return ret_val

    def punsubscribe(self, *args):
        """
        Unsubscribe from the supplied patterns. If empty, unsubscribe from
        all patterns.
        """
        if args:
            args = list_or_args(args[0], args[1:])
        retval = self.execute_command('PUNSUBSCRIBE', *args)
        if args:
            patterns = self._normalize_keys(dict.fromkeys(args))
        else:
            patterns = self.patterns
        self.pending_unsubscribe_patterns.update(patterns)
        return retval

    def subscribe(self, *args, **kwargs):
        """
        Subscribe to channels. Channels supplied as keyword arguments expect
        a channel name as the key and a callable as the value. A channel's
        callable will be invoked automatically when a message is received on
        that channel rather than producing a message via ``listen()`` or
        ``get_message()``.
        """
        if args:
            args = list_or_args(args[0], args[1:])
        new_channels = dict.fromkeys(args)
        new_channels.update(kwargs)
        ret_val = self.execute_command('SUBSCRIBE', *iterkeys(new_channels))
        # update the channels dict AFTER we send the command. we don't want to
        # subscribe twice to these channels, once for the command and again
        # for the reconnection.
        new_channels = self._normalize_keys(new_channels)
        self.channels.update(new_channels)
        self.pending_unsubscribe_channels.difference_update(new_channels)
        return ret_val

    def unsubscribe(self, *args):
        """
        Unsubscribe from the supplied channels. If empty, unsubscribe from
        all channels
        """
        if args:
            args = list_or_args(args[0], args[1:])
        retval = self.execute_command('UNSUBSCRIBE', *args)
        if args:
            channels = self._normalize_keys(dict.fromkeys(args))
        else:
            channels = self.channels
        self.pending_unsubscribe_channels.update(channels)
        return retval

    def listen(self):
        "Listen for messages on channels this client has been subscribed to"
        while self.subscribed:
            response = self.handle_message(self.parse_response(block=True))
            if response is not None:
                yield response

    def get_message(self, ignore_subscribe_messages=False, timeout=0):
        """
        Get the next message if one is available, otherwise None.

        If timeout is specified, the system will wait for `timeout` seconds
        before returning. Timeout should be specified as a floating point
        number.
        """
        response = self.parse_response(block=False, timeout=timeout)
        if response:
            return self.handle_message(response, ignore_subscribe_messages)
        return None

    def ping(self, message=None):
        """
        Ping the Redis server
        """
        message = '' if message is None else message
        return self.execute_command('PING', message)

    def handle_message(self, response, ignore_subscribe_messages=False):
        """
        Parses a pub/sub message. If the channel or pattern was subscribed to
        with a message handler, the handler is invoked instead of a parsed
        message being returned.
        """
        message_type = nativestr(response[0])
        if message_type == 'pmessage':
            message = {
                'type': message_type,
                'pattern': response[1],
                'channel': response[2],
                'data': response[3]
            }
        elif message_type == 'pong':
            message = {
                'type': message_type,
                'pattern': None,
                'channel': None,
                'data': response[1]
            }
        else:
            message = {
                'type': message_type,
                'pattern': None,
                'channel': response[1],
                'data': response[2]
            }
        # if this is an unsubscribe message, remove it from memory
        if message_type in self.UNSUBSCRIBE_MESSAGE_TYPES:
            if message_type == 'punsubscribe':
                pattern = response[1]
                if pattern in self.pending_unsubscribe_patterns:
                    self.pending_unsubscribe_patterns.remove(pattern)
                    self.patterns.pop(pattern, None)
            else:
                channel = response[1]
                if channel in self.pending_unsubscribe_channels:
                    self.pending_unsubscribe_channels.remove(channel)
                    self.channels.pop(channel, None)

        if message_type in self.PUBLISH_MESSAGE_TYPES:
            # if there's a message handler, invoke it
            if message_type == 'pmessage':
                handler = self.patterns.get(message['pattern'], None)
            else:
                handler = self.channels.get(message['channel'], None)
            if handler:
                handler(message)
                return None
        elif message_type != 'pong':
            # this is a subscribe/unsubscribe message. ignore if we don't
            # want them
            if ignore_subscribe_messages or self.ignore_subscribe_messages:
                return None

        return message

    def run_in_thread(self, sleep_time=0, daemon=False):
        for channel, handler in iteritems(self.channels):
            if handler is None:
                raise PubSubError("Channel: '%s' has no handler registered" %
                                  channel)
        for pattern, handler in iteritems(self.patterns):
            if handler is None:
                raise PubSubError("Pattern: '%s' has no handler registered" %
                                  pattern)

        thread = PubSubWorkerThread(self, sleep_time, daemon=daemon)
        thread.start()
        return thread


class PubSubWorkerThread(threading.Thread):
    def __init__(self, pubsub, sleep_time, daemon=False):
        super(PubSubWorkerThread, self).__init__()
        self.daemon = daemon
        self.pubsub = pubsub
        self.sleep_time = sleep_time
        self._running = False

    def run(self):
        if self._running:
            return
        self._running = True
        pubsub = self.pubsub
        sleep_time = self.sleep_time
        while pubsub.subscribed:
            pubsub.get_message(ignore_subscribe_messages=True,
                               timeout=sleep_time)
        pubsub.close()
        self._running = False

    def stop(self):
        # stopping simply unsubscribes from all channels and patterns.
        # the unsubscribe responses that are generated will short circuit
        # the loop in run(), calling pubsub.close() to clean up the connection
        self.pubsub.unsubscribe()
        self.pubsub.punsubscribe()


class Pipeline(Redis):
    """
    Pipelines provide a way to transmit multiple commands to the Redis server
    in one transmission.  This is convenient for batch processing, such as
    saving all the values in a list to Redis.

    All commands executed within a pipeline are wrapped with MULTI and EXEC
    calls. This guarantees all commands executed in the pipeline will be
    executed atomically.

    Any command raising an exception does *not* halt the execution of
    subsequent commands in the pipeline. Instead, the exception is caught
    and its instance is placed into the response list returned by execute().
    Code iterating over the response list should be able to deal with an
    instance of an exception as a potential value. In general, these will be
    ResponseError exceptions, such as those raised when issuing a command
    on a key of a different datatype.
    """

    UNWATCH_COMMANDS = {'DISCARD', 'EXEC', 'UNWATCH'}

    def __init__(self, connection_pool, response_callbacks, transaction,
                 shard_hint):
        self.connection_pool = connection_pool
        self.connection = None
        self.response_callbacks = response_callbacks
        self.transaction = transaction
        self.shard_hint = shard_hint

        self.watching = False
        self.reset()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.reset()

    def __del__(self):
        try:
            self.reset()
        except Exception:
            pass

    def __len__(self):
        return len(self.command_stack)

    def reset(self):
        self.command_stack = []
        self.scripts = set()
        # make sure to reset the connection state in the event that we were
        # watching something
        if self.watching and self.connection:
            try:
                # call this manually since our unwatch or
                # immediate_execute_command methods can call reset()
                self.connection.send_command('UNWATCH')
                self.connection.read_response()
            except ConnectionError:
                # disconnect will also remove any previous WATCHes
                self.connection.disconnect()
        # clean up the other instance attributes
        self.watching = False
        self.explicit_transaction = False
        # we can safely return the connection to the pool here since we're
        # sure we're no longer WATCHing anything
        if self.connection:
            self.connection_pool.release(self.connection)
            self.connection = None

    def multi(self):
        """
        Start a transactional block of the pipeline after WATCH commands
        are issued. End the transactional block with `execute`.
        """
        if self.explicit_transaction:
            raise RedisError('Cannot issue nested calls to MULTI')
        if self.command_stack:
            raise RedisError('Commands without an initial WATCH have already '
                             'been issued')
        self.explicit_transaction = True

    def execute_command(self, *args, **kwargs):
        if (self.watching or args[0] == 'WATCH') and \
                not self.explicit_transaction:
            return self.immediate_execute_command(*args, **kwargs)
        return self.pipeline_execute_command(*args, **kwargs)

    def immediate_execute_command(self, *args, **options):
        """
        Execute a command immediately, but don't auto-retry on a
        ConnectionError if we're already WATCHing a variable. Used when
        issuing WATCH or subsequent commands retrieving their values but before
        MULTI is called.
        """
        command_name = args[0]
        conn = self.connection
        # if this is the first call, we need a connection
        if not conn:
            conn = self.connection_pool.get_connection(command_name,
                                                       self.shard_hint)
            self.connection = conn
        try:
            conn.send_command(*args)
            return self.parse_response(conn, command_name, **options)
        except (ConnectionError, TimeoutError) as e:
            conn.disconnect()
            if not conn.retry_on_timeout and isinstance(e, TimeoutError):
                raise
            # if we're not already watching, we can safely retry the command
            try:
                if not self.watching:
                    conn.send_command(*args)
                    return self.parse_response(conn, command_name, **options)
            except ConnectionError:
                # the retry failed so cleanup.
                conn.disconnect()
                self.reset()
                raise

    def pipeline_execute_command(self, *args, **options):
        """
        Stage a command to be executed when execute() is next called

        Returns the current Pipeline object back so commands can be
        chained together, such as:

        pipe = pipe.set('foo', 'bar').incr('baz').decr('bang')

        At some other point, you can then run: pipe.execute(),
        which will execute all commands queued in the pipe.
        """
        self.command_stack.append((args, options))
        return self

    def _execute_transaction(self, connection, commands, raise_on_error):
        cmds = chain([(('MULTI', ), {})], commands, [(('EXEC', ), {})])
        all_cmds = connection.pack_commands([args for args, options in cmds
                                             if EMPTY_RESPONSE not in options])
        connection.send_packed_command(all_cmds)
        errors = []

        # parse off the response for MULTI
        # NOTE: we need to handle ResponseErrors here and continue
        # so that we read all the additional command messages from
        # the socket
        try:
            self.parse_response(connection, '_')
        except ResponseError:
            errors.append((0, sys.exc_info()[1]))

        # and all the other commands
        for i, command in enumerate(commands):
            if EMPTY_RESPONSE in command[1]:
                errors.append((i, command[1][EMPTY_RESPONSE]))
            else:
                try:
                    self.parse_response(connection, '_')
                except ResponseError:
                    ex = sys.exc_info()[1]
                    self.annotate_exception(ex, i + 1, command[0])
                    errors.append((i, ex))

        # parse the EXEC.
        try:
            response = self.parse_response(connection, '_')
        except ExecAbortError:
            if self.explicit_transaction:
                self.immediate_execute_command('DISCARD')
            if errors:
                raise errors[0][1]
            raise sys.exc_info()[1]

        if response is None:
            raise WatchError("Watched variable changed.")

        # put any parse errors into the response
        for i, e in errors:
            response.insert(i, e)

        if len(response) != len(commands):
            self.connection.disconnect()
            raise ResponseError("Wrong number of response items from "
                                "pipeline execution")

        # find any errors in the response and raise if necessary
        if raise_on_error:
            self.raise_first_error(commands, response)

        # We have to run response callbacks manually
        data = []
        for r, cmd in izip(response, commands):
            if not isinstance(r, Exception):
                args, options = cmd
                command_name = args[0]
                if command_name in self.response_callbacks:
                    r = self.response_callbacks[command_name](r, **options)
            data.append(r)
        return data

    def _execute_pipeline(self, connection, commands, raise_on_error):
        # build up all commands into a single request to increase network perf
        all_cmds = connection.pack_commands([args for args, _ in commands])
        connection.send_packed_command(all_cmds)

        response = []
        for args, options in commands:
            try:
                response.append(
                    self.parse_response(connection, args[0], **options))
            except ResponseError:
                response.append(sys.exc_info()[1])

        if raise_on_error:
            self.raise_first_error(commands, response)
        return response

    def raise_first_error(self, commands, response):
        for i, r in enumerate(response):
            if isinstance(r, ResponseError):
                self.annotate_exception(r, i + 1, commands[i][0])
                raise r

    def annotate_exception(self, exception, number, command):
        cmd = ' '.join(imap(safe_unicode, command))
        msg = 'Command # %d (%s) of pipeline caused error: %s' % (
            number, cmd, safe_unicode(exception.args[0]))
        exception.args = (msg,) + exception.args[1:]

    def parse_response(self, connection, command_name, **options):
        result = Redis.parse_response(
            self, connection, command_name, **options)
        if command_name in self.UNWATCH_COMMANDS:
            self.watching = False
        elif command_name == 'WATCH':
            self.watching = True
        return result

    def load_scripts(self):
        # make sure all scripts that are about to be run on this pipeline exist
        scripts = list(self.scripts)
        immediate = self.immediate_execute_command
        shas = [s.sha for s in scripts]
        # we can't use the normal script_* methods because they would just
        # get buffered in the pipeline.
        exists = immediate('SCRIPT EXISTS', *shas)
        if not all(exists):
            for s, exist in izip(scripts, exists):
                if not exist:
                    s.sha = immediate('SCRIPT LOAD', s.script)

    def execute(self, raise_on_error=True):
        "Execute all the commands in the current pipeline"
        stack = self.command_stack
        if not stack:
            return []
        if self.scripts:
            self.load_scripts()
        if self.transaction or self.explicit_transaction:
            execute = self._execute_transaction
        else:
            execute = self._execute_pipeline

        conn = self.connection
        if not conn:
            conn = self.connection_pool.get_connection('MULTI',
                                                       self.shard_hint)
            # assign to self.connection so reset() releases the connection
            # back to the pool after we're done
            self.connection = conn

        try:
            return execute(conn, stack, raise_on_error)
        except (ConnectionError, TimeoutError) as e:
            conn.disconnect()
            if not conn.retry_on_timeout and isinstance(e, TimeoutError):
                raise
            # if we were watching a variable, the watch is no longer valid
            # since this connection has died. raise a WatchError, which
            # indicates the user should retry his transaction. If this is more
            # than a temporary failure, the WATCH that the user next issues
            # will fail, propegating the real ConnectionError
            if self.watching:
                raise WatchError("A ConnectionError occured on while watching "
                                 "one or more keys")
            # otherwise, it's safe to retry since the transaction isn't
            # predicated on any state
            return execute(conn, stack, raise_on_error)
        finally:
            self.reset()

    def watch(self, *names):
        "Watches the values at keys ``names``"
        if self.explicit_transaction:
            raise RedisError('Cannot issue a WATCH after a MULTI')
        return self.execute_command('WATCH', *names)

    def unwatch(self):
        "Unwatches all previously specified keys"
        return self.watching and self.execute_command('UNWATCH') or True


class Script(object):
    "An executable Lua script object returned by ``register_script``"

    def __init__(self, registered_client, script):
        self.registered_client = registered_client
        self.script = script
        # Precalculate and store the SHA1 hex digest of the script.

        if isinstance(script, basestring):
            # We need the encoding from the client in order to generate an
            # accurate byte representation of the script
            encoder = registered_client.connection_pool.get_encoder()
            script = encoder.encode(script)
        self.sha = hashlib.sha1(script).hexdigest()

    def __call__(self, keys=[], args=[], client=None):
        "Execute the script, passing any required ``args``"
        if client is None:
            client = self.registered_client
        args = tuple(keys) + tuple(args)
        # make sure the Redis server knows about the script
        if isinstance(client, Pipeline):
            # Make sure the pipeline can register the script before executing.
            client.scripts.add(self)
        try:
            return client.evalsha(self.sha, len(keys), *args)
        except NoScriptError:
            # Maybe the client is pointed to a differnet server than the client
            # that created this instance?
            # Overwrite the sha just in case there was a discrepancy.
            self.sha = client.script_load(self.script)
            return client.evalsha(self.sha, len(keys), *args)


class BitFieldOperation(object):
    """
    Command builder for BITFIELD commands.
    """
    def __init__(self, client, key, default_overflow=None):
        self.client = client
        self.key = key
        self._default_overflow = default_overflow
        self.reset()

    def reset(self):
        """
        Reset the state of the instance to when it was constructed
        """
        self.operations = []
        self._last_overflow = 'WRAP'
        self.overflow(self._default_overflow or self._last_overflow)

    def overflow(self, overflow):
        """
        Update the overflow algorithm of successive INCRBY operations
        :param overflow: Overflow algorithm, one of WRAP, SAT, FAIL. See the
            Redis docs for descriptions of these algorithmsself.
        :returns: a :py:class:`BitFieldOperation` instance.
        """
        overflow = overflow.upper()
        if overflow != self._last_overflow:
            self._last_overflow = overflow
            self.operations.append(('OVERFLOW', overflow))
        return self

    def incrby(self, fmt, offset, increment, overflow=None):
        """
        Increment a bitfield by a given amount.
        :param fmt: format-string for the bitfield being updated, e.g. 'u8'
            for an unsigned 8-bit integer.
        :param offset: offset (in number of bits). If prefixed with a
            '#', this is an offset multiplier, e.g. given the arguments
            fmt='u8', offset='#2', the offset will be 16.
        :param int increment: value to increment the bitfield by.
        :param str overflow: overflow algorithm. Defaults to WRAP, but other
            acceptable values are SAT and FAIL. See the Redis docs for
            descriptions of these algorithms.
        :returns: a :py:class:`BitFieldOperation` instance.
        """
        if overflow is not None:
            self.overflow(overflow)

        self.operations.append(('INCRBY', fmt, offset, increment))
        return self

    def get(self, fmt, offset):
        """
        Get the value of a given bitfield.
        :param fmt: format-string for the bitfield being read, e.g. 'u8' for
            an unsigned 8-bit integer.
        :param offset: offset (in number of bits). If prefixed with a
            '#', this is an offset multiplier, e.g. given the arguments
            fmt='u8', offset='#2', the offset will be 16.
        :returns: a :py:class:`BitFieldOperation` instance.
        """
        self.operations.append(('GET', fmt, offset))
        return self

    def set(self, fmt, offset, value):
        """
        Set the value of a given bitfield.
        :param fmt: format-string for the bitfield being read, e.g. 'u8' for
            an unsigned 8-bit integer.
        :param offset: offset (in number of bits). If prefixed with a
            '#', this is an offset multiplier, e.g. given the arguments
            fmt='u8', offset='#2', the offset will be 16.
        :param int value: value to set at the given position.
        :returns: a :py:class:`BitFieldOperation` instance.
        """
        self.operations.append(('SET', fmt, offset, value))
        return self

    @property
    def command(self):
        cmd = ['BITFIELD', self.key]
        for ops in self.operations:
            cmd.extend(ops)
        return cmd

    def execute(self):
        """
        Execute the operation(s) in a single BITFIELD command. The return value
        is a list of values corresponding to each operation. If the client
        used to create this instance was a pipeline, the list of values
        will be present within the pipeline's execute.
        """
        command = self.command
        self.reset()
        return self.client.execute_command(*command)
