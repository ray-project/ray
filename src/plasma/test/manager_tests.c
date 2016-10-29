#include "greatest.h"

#include <assert.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>

#include "common.h"
#include "event_loop.h"
#include "io.h"

#include "plasma.h"
#include "plasma_client.h"
#include "plasma_manager.h"

SUITE(plasma_manager_tests);

const char *manager_addr = "127.0.0.1";
int manager_port = 12345;
const char *store_socket_name = "/tmp/store12345";
const char *manager_socket_name = "/tmp/manager12345";
object_id oid;

int test_done_handler(event_loop *loop, timer_id id, void *context) {
  event_loop_stop(loop);
  return AE_NOMORE;
}

typedef struct {
  int port;
  /** Connection to the manager's TCP socket. */
  int manager_remote_fd;
  /** Connection to the manager's Unix socket. */
  int manager_local_fd;
  int local_store;
  int manager;
  plasma_manager_state *state;
  event_loop *loop;
  /* Accept a connection from the local manager on the remote manager. */
  client_connection *write_conn;
  client_connection *read_conn;
  /* Connect a new client to the local plasma manager and mock a request to an
   * object. */
  plasma_connection *plasma_conn;
  client_connection *client_conn;
  client_object_connection *object_conn;
} plasma_mock;

plasma_mock *init_plasma_mock(int port, plasma_mock *remote_mock) {
  plasma_mock *mock = malloc(sizeof(plasma_mock));
  /* Start listening on all the ports and initiate the local plasma manager. */
  mock->port = port;
  mock->manager_remote_fd = bind_inet_sock(port);
  mock->manager_local_fd = bind_ipc_sock(manager_socket_name);
  mock->local_store = bind_ipc_sock(store_socket_name);
  mock->state =
      init_plasma_manager_state(store_socket_name, manager_addr, port, NULL, 0);
  mock->loop = get_event_loop(mock->state);
  /* Accept a connection from the local manager on the remote manager. */
  if (remote_mock != NULL) {
    mock->write_conn =
        get_manager_connection(remote_mock->state, manager_addr, port);
    mock->read_conn = new_client_connection(mock->loop, mock->manager_remote_fd,
                                            mock->state, 0);
  } else {
    mock->write_conn = NULL;
    mock->read_conn = NULL;
  }

  mock->plasma_conn = NULL;
  mock->client_conn = NULL;
  mock->object_conn = NULL;
  return mock;
}

void add_mock_object_conn(plasma_mock *mock, object_id oid) {
  /* Connect a new client to the local plasma manager and mock a request to an
   * object. */
  mock->plasma_conn = plasma_connect(store_socket_name, manager_socket_name);
  mock->client_conn =
      new_client_connection(mock->loop, mock->manager_local_fd, mock->state, 0);
  mock->object_conn = add_object_connection(mock->client_conn, oid);
}

void destroy_plasma_mock(plasma_mock *mock) {
  if (mock->object_conn != NULL) {
    free(mock->client_conn);
    free(mock->plasma_conn);
  }
  if (mock->read_conn != NULL) {
    close(get_client_sock(mock->read_conn));
    free(mock->read_conn);
  }
  destroy_plasma_manager_state(mock->state);
  close(mock->local_store);
  close(mock->manager_local_fd);
  close(mock->manager_remote_fd);
  free(mock);
}

/**
 * This test checks correct behavior of request_transfer in a non-failure
 * scenario. Specifically, when one plasma manager calls request_transfer, the
 * correct remote manager should receive the correct message. The test:
 * - Buffer a transfer request for the remote manager.
 * - Start and stop the event loop to make sure that we send the buffered
 *   request.
 * - Expect to see a PLASMA_TRANSFER message on the remote manager with the
 *   correct object ID.
 */
TEST request_transfer_test(void) {
  plasma_mock *local_mock = init_plasma_mock(manager_port, NULL);
  add_mock_object_conn(local_mock, oid);
  plasma_mock *remote_mock = init_plasma_mock(12346, local_mock);
  const char **manager_vector = malloc(sizeof(char *));
  manager_vector[0] = "127.0.0.1:12346";
  request_transfer(oid, 1, manager_vector, local_mock->client_conn);
  event_loop_add_timer(local_mock->loop, MANAGER_TIMEOUT, test_done_handler,
                       local_mock->state);
  event_loop_run(local_mock->loop);
  int64_t type;
  int64_t length;
  plasma_request *req;
  int read_fd = get_client_sock(remote_mock->read_conn);
  read_message(read_fd, &type, &length, (uint8_t **) &req);
  ASSERT(type == PLASMA_TRANSFER);
  ASSERT(req->num_object_ids == 1);
  ASSERT(memcmp(&oid, &req->object_ids[0], sizeof(object_id)) == 0);
  /* Clean up. */
  free(req);
  destroy_plasma_mock(remote_mock);
  remove_object_connection(local_mock->client_conn, local_mock->object_conn);
  destroy_plasma_mock(local_mock);
  PASS();
}

/**
 * This test checks correct behavior of request_transfer in a scenario when the
 * first manager we try times out. Specifically, when one plasma manager calls
 * request_transfer on a list of remote managers and the first manager isn't
 * reachable, the second remote manager should receive the correct message
 * after the timeout. The test:
 * - Buffer a transfer request for the remote managers.
 * - Start and stop the event loop after a timeout to make sure that we
 *   trigger the timeout on the first manager.
 * - Expect to see a PLASMA_TRANSFER message on the second remote manager
 *   with the correct object ID.
 */
TEST request_transfer_retry_test(void) {
  plasma_mock *local_mock = init_plasma_mock(manager_port, NULL);
  add_mock_object_conn(local_mock, oid);
  plasma_mock *remote_mock1 = init_plasma_mock(12346, local_mock);
  plasma_mock *remote_mock2 = init_plasma_mock(12347, local_mock);
  const char **manager_vector = malloc(sizeof(char *) * 2);
  manager_vector[0] = "127.0.0.1:12346";
  manager_vector[1] = "127.0.0.1:12347";
  request_transfer(oid, 2, manager_vector, local_mock->client_conn);
  event_loop_add_timer(local_mock->loop, MANAGER_TIMEOUT * 2, test_done_handler,
                       local_mock->state);
  event_loop_run(local_mock->loop);

  int64_t type;
  int64_t length;
  plasma_request *req;
  int read_fd = get_client_sock(remote_mock2->read_conn);
  read_message(read_fd, &type, &length, (uint8_t **) &req);
  ASSERT(type == PLASMA_TRANSFER);
  ASSERT(req->num_object_ids == 1);
  ASSERT(memcmp(&oid, &req->object_ids[0], sizeof(object_id)) == 0);
  /* Clean up. */
  free(req);
  destroy_plasma_mock(remote_mock2);
  destroy_plasma_mock(remote_mock1);
  remove_object_connection(local_mock->client_conn, local_mock->object_conn);
  destroy_plasma_mock(local_mock);
  PASS();
}

/**
 * This test checks correct behavior of request_transfer in a failure scenario.
 * Specifically, when one plasma manager calls request_transfer, and the remote
 * manager that holds the object is unreachable, the client should receive the
 * failure message after all the retries have timed out.
 * - Buffer a transfer request for the remote manager.
 * - Start and stop the event loop after NUM_RETRIES timeouts to make sure that
 *   we trigger all the retries.
 * - Expect to see a response on the plasma client saying that the object
 *   wasn't fetched.
 */
TEST request_transfer_timeout_test(void) {
  plasma_mock *local_mock = init_plasma_mock(manager_port, NULL);
  add_mock_object_conn(local_mock, oid);
  plasma_mock *remote_mock = init_plasma_mock(12346, local_mock);
  const char **manager_vector = malloc(sizeof(char *));
  manager_vector[0] = "127.0.0.1:12346";
  request_transfer(oid, 1, manager_vector, local_mock->client_conn);
  event_loop_add_timer(local_mock->loop, MANAGER_TIMEOUT * (NUM_RETRIES + 2),
                       test_done_handler, local_mock->state);
  event_loop_run(local_mock->loop);

  plasma_reply reply;
  int manager_fd = get_manager_fd(local_mock->plasma_conn);
  int nbytes = recv(manager_fd, (uint8_t *) &reply, sizeof(reply), MSG_WAITALL);
  ASSERT_EQ(nbytes, sizeof(reply));
  ASSERT_EQ(memcmp(&oid, &reply.object_id, sizeof(object_id)), 0);
  ASSERT_EQ(reply.has_object, 0);
  /* Clean up. */
  destroy_plasma_mock(remote_mock);
  destroy_plasma_mock(local_mock);
  PASS();
}

/**
 * This test checks correct behavior of reading and writing an object chunk
 * from one manager to another.
 * - Write a one-chunk object from the local to the remote manager.
 * - Read the object chunk on the remote manager.
 * - Expect to see the same data.
 */
TEST read_write_object_chunk_test(void) {
  plasma_mock *local_mock = init_plasma_mock(manager_port, NULL);
  plasma_mock *remote_mock = init_plasma_mock(12346, local_mock);
  /* Create a mock object buffer to transfer. */
  const char *data = "Hello world!";
  const int data_size = strlen(data) + 1;
  const int metadata_size = 0;
  plasma_request_buffer remote_buf = {
      .type = PLASMA_DATA,
      .object_id = oid,
      .data = (uint8_t *) data,
      .data_size = data_size,
      .metadata = (uint8_t *) data + data_size,
      .metadata_size = metadata_size,
  };
  plasma_request_buffer local_buf = {
      .object_id = oid,
      .data_size = data_size,
      .metadata_size = metadata_size,
      .data = malloc(data_size),
  };
  /* The test:
   * - Write the object data from the remote manager to the local.
   * - Read the object data on the local manager.
   * - Check that the data matches.
   */
  write_object_chunk(remote_mock->write_conn, &remote_buf);
  int done = read_object_chunk(remote_mock->read_conn, &local_buf);
  ASSERT(done);
  ASSERT_EQ(memcmp(remote_buf.data, local_buf.data, data_size), 0);
  /* Clean up. */
  free(local_buf.data);
  destroy_plasma_mock(remote_mock);
  destroy_plasma_mock(local_mock);
  PASS();
}

SUITE(plasma_manager_tests) {
  memset(&oid, 1, sizeof(object_id));
  RUN_TEST(request_transfer_test);
  RUN_TEST(request_transfer_retry_test);
  RUN_TEST(request_transfer_timeout_test);
  RUN_TEST(read_write_object_chunk_test);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
  GREATEST_MAIN_BEGIN();
  RUN_SUITE(plasma_manager_tests);
  GREATEST_MAIN_END();
}
