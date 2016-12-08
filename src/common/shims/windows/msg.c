#include <sys/socket.h>

#pragma comment(lib, "IPHlpAPI.lib")

struct _MIB_TCPROW2 {
  DWORD dwState, dwLocalAddr, dwLocalPort, dwRemoteAddr, dwRemotePort,
      dwOwningPid;
  enum _TCP_CONNECTION_OFFLOAD_STATE dwOffloadState;
};

struct _MIB_TCPTABLE2 {
  DWORD dwNumEntries;
  struct _MIB_TCPROW2 table[1];
};

DECLSPEC_IMPORT ULONG WINAPI GetTcpTable2(struct _MIB_TCPTABLE2 *TcpTable,
                                          PULONG SizePointer,
                                          BOOL Order);

static DWORD getsockpid(int client) {
  /* http://stackoverflow.com/a/25431340 */
  DWORD pid = 0;

  struct sockaddr_in Server = {0};
  int ServerSize = sizeof(Server);

  struct sockaddr_in Client = {0};
  int ClientSize = sizeof(Client);

  if ((getsockname(client, (struct sockaddr *) &Server, &ServerSize) == 0) &&
      (getpeername(client, (struct sockaddr *) &Client, &ClientSize) == 0)) {
    struct _MIB_TCPTABLE2 *TcpTable = NULL;
    ULONG TcpTableSize = 0;
    ULONG result;
    do {
      result = GetTcpTable2(TcpTable, &TcpTableSize, TRUE);
      if (result != ERROR_INSUFFICIENT_BUFFER) {
        break;
      }
      free(TcpTable);
      TcpTable = (struct _MIB_TCPTABLE2 *) malloc(TcpTableSize);
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

ssize_t sendmsg(int sockfd, struct msghdr *msg, int flags) {
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
    int *const pfd = (int *) CMSG_DATA(header);
    msg->msg_control = NULL;
    msg->msg_controllen = 0;
    WSAPROTOCOL_INFO protocol_info = {0};
    BOOL const is_socket = !!FDAPI_GetSocketStatePtr(*pfd);
    DWORD const target_pid = getsockpid(sockfd);
    HANDLE target_process = NULL;
    if (target_pid) {
      if (!is_socket) {
        /* This is a regular handle... fit it into the same struct */
        target_process = OpenProcess(PROCESS_DUP_HANDLE, FALSE, target_pid);
        if (target_process) {
          if (DuplicateHandle(GetCurrentProcess(), (HANDLE)(intptr_t) *pfd,
                              target_process, (HANDLE *) &protocol_info, 0,
                              TRUE, DUPLICATE_SAME_ACCESS)) {
            result = 0;
          }
        }
      } else {
        /* This is a socket... */
        result = FDAPI_WSADuplicateSocket(*pfd, target_pid, &protocol_info);
      }
    }
    if (result != -1) {
      int const nbufs = msg->dwBufferCount + 1;
      WSABUF *const bufs =
          (struct _WSABUF *) _alloca(sizeof(*msg->lpBuffers) * nbufs);
      bufs[0].buf = (char *) &protocol_info;
      bufs[0].len = sizeof(protocol_info);
      memcpy(&bufs[1], msg->lpBuffers,
             msg->dwBufferCount * sizeof(*msg->lpBuffers));
      DWORD nb;
      msg->lpBuffers = bufs;
      msg->dwBufferCount = nbufs;
      result = FDAPI_WSASend(sockfd, bufs, nbufs, &nb, flags | msg->dwFlags,
                             NULL, NULL);
      if (result != -1) {
        result = (ssize_t)(nb - sizeof(protocol_info));
      }
    }
    if (result == -1 && target_process && !is_socket) {
      /* we failed to send the handle, and it needs cleaning up! */
      HANDLE duplicated_back = NULL;
      if (DuplicateHandle(target_process, *(HANDLE *) &protocol_info,
                          GetCurrentProcess(), &duplicated_back, 0, FALSE,
                          DUPLICATE_CLOSE_SOURCE)) {
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

ssize_t recvmsg(int sockfd, struct msghdr *msg, int flags) {
  int result = -1;
  struct cmsghdr *header = CMSG_FIRSTHDR(msg);
  if (msg->msg_controllen) {
    struct msghdr const old_msg = *msg;
    msg->msg_control = NULL;
    msg->msg_controllen = 0;
    WSAPROTOCOL_INFO protocol_info = {0};
    int const nbufs = msg->dwBufferCount + 1;
    WSABUF *const bufs =
        (struct _WSABUF *) _alloca(sizeof(*msg->lpBuffers) * nbufs);
    bufs[0].buf = (char *) &protocol_info;
    bufs[0].len = sizeof(protocol_info);
    memcpy(&bufs[1], msg->lpBuffers,
           msg->dwBufferCount * sizeof(*msg->lpBuffers));
    DWORD nb;
    DWORD dwFlags = msg->dwFlags | flags;
    fd_set fds;
    FD_ZERO(&fds);
    FD_SET(sockfd, &fds);
    /* Unfortunately FDAPI_SocketAttachIOCP() sets FIONBIO (non-blocking mode),
       so we should wait...
       TODO: BUG: We are assuming we won't need to loop here.
       That's likely to be the case for what we care about, but not true in
       general.
    */
    result = select(1, &fds, NULL, NULL, NULL);
    if (result != -1) {
      result = FDAPI_WSARecv(sockfd, bufs, nbufs, &nb, &dwFlags, NULL, NULL);
    }
    if (result != -1) {
      result = (ssize_t)(nb - sizeof(protocol_info));
    }
    *msg = old_msg;
    msg->dwFlags = dwFlags;
    if (result != -1) {
      int *const pfd = (int *) CMSG_DATA(header);
      if (protocol_info.iSocketType == 0 && protocol_info.iProtocol == 0) {
        *pfd = *(int *) &protocol_info;
      } else {
        *pfd = FDAPI_WSASocket(FROM_PROTOCOL_INFO, FROM_PROTOCOL_INFO,
                               FROM_PROTOCOL_INFO, &protocol_info, 0, 0);
      }
      header->cmsg_len = CMSG_LEN(sizeof(int));
      header->cmsg_level = SOL_SOCKET;
      header->cmsg_type = SCM_RIGHTS;
    }
  }
  return result;
}
