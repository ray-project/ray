#include "plasma.h"

#include "io.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

plasma_request plasma_make_request(object_id object_id) {
  plasma_request request;
  memset(&request, 0, sizeof(request));
  request.num_object_ids = 1;
  request.object_ids[0] = object_id;
  return request;
}

plasma_request *plasma_alloc_request(int num_object_ids,
                                     object_id object_ids[]) {
  DCHECK(num_object_ids >= 1);
  int req_size = plasma_request_size(num_object_ids);
  plasma_request *req = malloc(req_size);
  memset(req, 0, req_size);
  req->num_object_ids = num_object_ids;
  memcpy(&req->object_ids, object_ids, num_object_ids * sizeof(object_ids[0]));
  return req;
}

void plasma_free_request(plasma_request *request) {
  free(request);
}

int64_t plasma_request_size(int num_object_ids) {
  int64_t object_ids_size = (num_object_ids - 1) * sizeof(object_id);
  return sizeof(plasma_request) + object_ids_size;
}

plasma_reply plasma_make_reply(object_id object_id) {
  plasma_reply reply;
  memset(&reply, 0, sizeof(reply));
  reply.num_object_ids = 1;
  reply.object_ids[0] = object_id;
  return reply;
}

plasma_reply *plasma_alloc_reply(int num_object_ids) {
  DCHECK(num_object_ids >= 1);
  int64_t size = plasma_reply_size(num_object_ids);
  plasma_reply *reply = malloc(size);
  memset(reply, 0, size);
  reply->num_object_ids = num_object_ids;
  return reply;
}

void plasma_free_reply(plasma_reply *reply) {
  free(reply);
}

int64_t plasma_reply_size(int num_object_ids) {
  DCHECK(num_object_ids >= 1);
  return sizeof(plasma_reply) + (num_object_ids - 1) * sizeof(object_id);
}

int plasma_send_reply(int sock, plasma_reply *reply) {
  DCHECK(reply);
  int64_t reply_size = plasma_reply_size(reply->num_object_ids);
  int n = write(sock, (uint8_t *) reply, reply_size);
  return n == reply_size ? 0 : -1;
}

int plasma_receive_reply(int sock, int64_t reply_size, plasma_reply *reply) {
  int r = recv(sock, reply, reply_size, 0);
  CHECKM(r != -1, "read error");
  CHECKM(r != 0, "connection disconnected");
  return r == reply_size ? 0 : -1;
}

int plasma_send_request(int sock, int64_t type, plasma_request *request) {
  DCHECK(request);
  int req_size = plasma_request_size(request->num_object_ids);
  int error = write_message(sock, type, req_size, (uint8_t *) request);
  return error ? -1 : 0;
}

int plasma_receive_request(int sock, int64_t *type, plasma_request **request) {
  int64_t length;
  read_message(sock, type, &length, (uint8_t **) request);
  if (*request == NULL) {
    return *type == DISCONNECT_CLIENT;
  }
  return length == plasma_request_size((*request)->num_object_ids) ? 0 : -1;
}
