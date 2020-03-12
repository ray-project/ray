#include <stdlib.h>
#include <sys/socket.h>

#include <Winsock2.h>  // include before other socket headers on Windows

#include <iphlpapi.h>
#include <tcpmib.h>

#pragma comment(lib, "IPHlpAPI.lib")

int socketpair(int domain, int type, int protocol, SOCKET sv[2]) {
  if ((domain != AF_UNIX && domain != AF_INET) || type != SOCK_STREAM) {
    return (int)INVALID_SOCKET;
  }
  SOCKET sockets[2];
  int r = dumb_socketpair(sockets);
  sv[0] = sockets[0];
  sv[1] = sockets[1];
  return r;
}

static DWORD getsockpid(SOCKET client) {
  /* http://stackoverflow.com/a/25431340 */
  DWORD pid = 0;

  struct sockaddr_in Server = {0};
  int ServerSize = sizeof(Server);

  struct sockaddr_in Client = {0};
  int ClientSize = sizeof(Client);

  if ((getsockname(client, (struct sockaddr *)&Server, &ServerSize) == 0) &&
      (getpeername(client, (struct sockaddr *)&Client, &ClientSize) == 0)) {
    struct _MIB_TCPTABLE2 *TcpTable = NULL;
    ULONG TcpTableSize = 0;
    ULONG result;
    do {
      result = GetTcpTable2(TcpTable, &TcpTableSize, TRUE);
      if (result != ERROR_INSUFFICIENT_BUFFER) {
        break;
      }
      free(TcpTable);
      TcpTable = (struct _MIB_TCPTABLE2 *)malloc(TcpTableSize);
    } while (TcpTable != NULL);

    if (result == NO_ERROR) {
      for (DWORD dw = 0; dw < TcpTable->dwNumEntries; ++dw) {
        struct _MIB_TCPROW2 *row = &(TcpTable->table[dw]);
        if ((row->dwState == 5 /* MIB_TCP_STATE_ESTAB */) &&
            (row->dwLocalAddr == Client.sin_addr.s_addr) &&
            ((row->dwLocalPort & 0xFFFF) == Client.sin_port) &&
            (row->dwRemoteAddr == Server.sin_addr.s_addr) &&
            ((row->dwRemotePort & 0xFFFF) == Server.sin_port)) {
          pid = row->dwOwningPid;
          break;
        }
      }
    }

    free(TcpTable);
  }

  return pid;
}

ssize_t sendmsg(SOCKET sock, struct msghdr *msg, int flags) {
  ssize_t result = -1;
  struct cmsghdr *header = CMSG_FIRSTHDR(msg);
  if (header->cmsg_level == SOL_SOCKET && header->cmsg_type == SCM_RIGHTS) {
    /* We're trying to send over a handle of some kind.
     * We have to look up which process we're communicating with,
     * open a handle to it, and then duplicate our handle into it.
     * However, the first two steps cannot be done atomically.
     * Therefore, this code HAS A RACE CONDITIONS and is therefore NOT SECURE.
     * In the absense of a malicious actor, though, it is exceedingly unlikely
     * that the child process closes AND that its process ID is reassigned
     * to another existing process.
     */
    struct msghdr const old_msg = *msg;
    SOCKET *const pfd = (SOCKET *)CMSG_DATA(header);
    msg->msg_control = NULL;
    msg->msg_controllen = 0;
    WSAPROTOCOL_INFO protocol_info = {0};
    /* assume socket if it's a pipe, until proven otherwise */
    BOOL is_socket = GetFileType((HANDLE)(SOCKET)(*pfd)) == FILE_TYPE_PIPE;
    DWORD const target_pid = getsockpid(sock);
    HANDLE target_process = NULL;
    if (target_pid) {
      if (is_socket) {
        result = WSADuplicateSocket(*pfd, target_pid, &protocol_info);
        if (result == -1 && WSAGetLastError() == WSAENOTSOCK) {
          is_socket = FALSE;
        }
      }
      if (!is_socket) {
        /* This is a regular handle... fit it into the same struct */
        target_process = OpenProcess(PROCESS_DUP_HANDLE, FALSE, target_pid);
        if (target_process) {
          if (DuplicateHandle(GetCurrentProcess(), (HANDLE)(intptr_t)*pfd, target_process,
                              (HANDLE *)&protocol_info, 0, TRUE, DUPLICATE_SAME_ACCESS)) {
            result = 0;
          }
        }
      }
    }
    if (result == 0) {
      int const nbufs = msg->dwBufferCount + 1;
      WSABUF *const bufs = (struct _WSABUF *)_alloca(sizeof(*msg->lpBuffers) * nbufs);
      bufs[0].buf = (char *)&protocol_info;
      bufs[0].len = sizeof(protocol_info);
      memcpy(&bufs[1], msg->lpBuffers, msg->dwBufferCount * sizeof(*msg->lpBuffers));
      DWORD nb;
      msg->lpBuffers = bufs;
      msg->dwBufferCount = nbufs;
      GUID wsaid_WSASendMsg = {
          0xa441e712, 0x754f, 0x43ca, {0x84, 0xa7, 0x0d, 0xee, 0x44, 0xcf, 0x60, 0x6d}};
      typedef INT PASCAL WSASendMsg_t(
          SOCKET s, LPWSAMSG lpMsg, DWORD dwFlags, LPDWORD lpNumberOfBytesSent,
          LPWSAOVERLAPPED lpOverlapped,
          LPWSAOVERLAPPED_COMPLETION_ROUTINE lpCompletionRoutine);
      WSASendMsg_t *WSASendMsg = NULL;
      result = WSAIoctl(sock, SIO_GET_EXTENSION_FUNCTION_POINTER, &wsaid_WSASendMsg,
                        sizeof(wsaid_WSASendMsg), &WSASendMsg, sizeof(WSASendMsg), &nb,
                        NULL, 0);
      if (result == 0) {
        result = (*WSASendMsg)(sock, msg, flags, &nb, NULL, NULL) == 0
                     ? (ssize_t)(nb - sizeof(protocol_info))
                     : 0;
      }
    }
    if (result != 0 && target_process && !is_socket) {
      /* we failed to send the handle, and it needs cleaning up! */
      HANDLE duplicated_back = NULL;
      if (DuplicateHandle(target_process, *(HANDLE *)&protocol_info, GetCurrentProcess(),
                          &duplicated_back, 0, FALSE, DUPLICATE_CLOSE_SOURCE)) {
        CloseHandle(duplicated_back);
      }
    }
    if (target_process) {
      CloseHandle(target_process);
    }
    *msg = old_msg;
  }
  return result;
}

ssize_t recvmsg(SOCKET sock, struct msghdr *msg, int flags) {
  int result = -1;
  struct cmsghdr *header = CMSG_FIRSTHDR(msg);
  if (msg->msg_controllen && flags == 0 /* We can't send flags on Windows... */) {
    struct msghdr const old_msg = *msg;
    msg->msg_control = NULL;
    msg->msg_controllen = 0;
    WSAPROTOCOL_INFO protocol_info = {0};
    int const nbufs = msg->dwBufferCount + 1;
    WSABUF *const bufs = (struct _WSABUF *)_alloca(sizeof(*msg->lpBuffers) * nbufs);
    bufs[0].buf = (char *)&protocol_info;
    bufs[0].len = sizeof(protocol_info);
    memcpy(&bufs[1], msg->lpBuffers, msg->dwBufferCount * sizeof(*msg->lpBuffers));
    typedef INT PASCAL WSARecvMsg_t(
        SOCKET s, LPWSAMSG lpMsg, LPDWORD lpNumberOfBytesRecvd,
        LPWSAOVERLAPPED lpOverlapped,
        LPWSAOVERLAPPED_COMPLETION_ROUTINE lpCompletionRoutine);
    WSARecvMsg_t *WSARecvMsg = NULL;
    DWORD nb;
    GUID wsaid_WSARecvMsg = {
        0xf689d7c8, 0x6f1f, 0x436b, {0x8a, 0x53, 0xe5, 0x4f, 0xe3, 0x51, 0xc3, 0x22}};
    result =
        WSAIoctl(sock, SIO_GET_EXTENSION_FUNCTION_POINTER, &wsaid_WSARecvMsg,
                 sizeof(wsaid_WSARecvMsg), &WSARecvMsg, sizeof(WSARecvMsg), &nb, NULL, 0);
    if (result == 0) {
      result = (*WSARecvMsg)(sock, msg, &nb, NULL, NULL) == 0
                   ? (ssize_t)(nb - sizeof(protocol_info))
                   : 0;
    }
    if (result == 0) {
      SOCKET *const pfd = (SOCKET *)CMSG_DATA(header);
      if (protocol_info.iSocketType == 0 && protocol_info.iProtocol == 0) {
        *pfd = *(SOCKET *)&protocol_info;
      } else {
        *pfd = WSASocket(FROM_PROTOCOL_INFO, FROM_PROTOCOL_INFO, FROM_PROTOCOL_INFO,
                         &protocol_info, 0, 0);
      }
      header->cmsg_level = SOL_SOCKET;
      header->cmsg_type = SCM_RIGHTS;
    }
    *msg = old_msg;
  }
  return result;
}
