#include "greatest.h"

#include <assert.h>
#include <unistd.h>
#include <poll.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <fcntl.h>

#include "common.h"
#include "test/test_common.h"
#include "event_loop.h"
#include "io.h"
#include "utstring.h"

#include "plasma.h"
#include "plasma_client.h"
#include "plasma_manager.h"
#include "plasma_protocol.h"

SUITE(plasma_manager_tests);

const char *plasma_store_socket_name = "/tmp/plasma_store_socket_1";
const char *plasma_manager_socket_name_format = "/tmp/plasma_manager_socket_%d";
const char *manager_addr = "127.0.0.1";
ObjectID oid;

void wait_for_pollin(int fd) {
  struct pollfd poll_list[1];
  poll_list[0].fd = fd;
  poll_list[0].events = POLLIN;
  int retval = poll(poll_list, (unsigned long) 1, -1);
  CHECK(retval > 0);
}

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
  ClientConnection *write_conn;
  ClientConnection *read_conn;
  /* Connect a new client to the local plasma manager and mock a request to an
   * object. */
  PlasmaConnection *plasma_conn;
  ClientConnection *client_conn;
} plasma_mock;

plasma_mock *init_plasma_mock(plasma_mock *remote_mock) {
  plasma_mock *mock = (plasma_mock *) malloc(sizeof(plasma_mock));
  /* Start listening on all the ports and initiate the local plasma manager. */
  mock->port = bind_inet_sock_retry(&mock->manager_remote_fd);
  mock->local_store = connect_ipc_sock_retry(plasma_store_socket_name, 5, 100);
  UT_string *manager_socket_name = bind_ipc_sock_retry(
      plasma_manager_socket_name_format, &mock->manager_local_fd);

  CHECK(mock->manager_local_fd >= 0 && mock->local_store >= 0);

  mock->state = init_plasma_manager_state(plasma_store_socket_name,
                                          utstring_body(manager_socket_name),
                                          manager_addr, mock->port, NULL, 0);
  mock->loop = get_event_loop(mock->state);
  /* Accept a connection from the local manager on the remote manager. */
  if (remote_mock != NULL) {
    mock->write_conn =
        get_manager_connection(remote_mock->state, manager_addr, mock->port);
    wait_for_pollin(mock->manager_remote_fd);
    mock->read_conn =
        ClientConnection_init(mock->loop, mock->manager_remote_fd, mock->state,
                              PLASMA_DEFAULT_RELEASE_DELAY);
  } else {
    mock->write_conn = NULL;
    mock->read_conn = NULL;
  }
  /* Connect a new client to the local plasma manager and mock a request to an
   * object. */
  mock->plasma_conn = plasma_connect(plasma_store_socket_name,
                                     utstring_body(manager_socket_name), 0);
  wait_for_pollin(mock->manager_local_fd);
  mock->client_conn =
      ClientConnection_init(mock->loop, mock->manager_local_fd, mock->state, 0);
  utstring_free(manager_socket_name);
  return mock;
}

void destroy_plasma_mock(plasma_mock *mock) {
  if (mock->read_conn != NULL) {
    close(get_client_sock(mock->read_conn));
    free(mock->read_conn);
  }
  destroy_plasma_manager_state(mock->state);
  free(mock->client_conn);
  plasma_disconnect(mock->plasma_conn);
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
 * - Expect to see a MessageType_PlasmaDataRequest message on the remote manager
 *   with the correct object ID.
 */
TEST request_transfer_test(void) {
  plasma_mock *local_mock = init_plasma_mock(NULL);
  plasma_mock *remote_mock = init_plasma_mock(local_mock);
  const char **manager_vector = (const char **) malloc(sizeof(char *));
  UT_string *addr = NULL;
  utstring_new(addr);
  utstring_printf(addr, "127.0.0.1:%d", remote_mock->port);
  manager_vector[0] = utstring_body(addr);
  call_request_transfer(oid, 1, manager_vector, local_mock->state);
  free(manager_vector);
  event_loop_add_timer(local_mock->loop, MANAGER_TIMEOUT, test_done_handler,
                       local_mock->state);
  event_loop_run(local_mock->loop);
  int read_fd = get_client_sock(remote_mock->read_conn);
  uint8_t *request_data =
      plasma_receive(read_fd, MessageType_PlasmaDataRequest);
  ObjectID oid2;
  char *address;
  int port;
  plasma_read_DataRequest(request_data, &oid2, &address, &port);
  ASSERT(ObjectID_equal(oid, oid2));
  free(address);
  /* Clean up. */
  utstring_free(addr);
  free(request_data);
  destroy_plasma_mock(remote_mock);
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
 * - Expect to see a MessageType_PlasmaDataRequest message on the second remote
 *   manager with the correct object ID.
 */
TEST request_transfer_retry_test(void) {
  plasma_mock *local_mock = init_plasma_mock(NULL);
  plasma_mock *remote_mock1 = init_plasma_mock(local_mock);
  plasma_mock *remote_mock2 = init_plasma_mock(local_mock);
  const char **manager_vector = (const char **) malloc(sizeof(char *) * 2);
  UT_string *addr0 = NULL;
  utstring_new(addr0);
  utstring_printf(addr0, "127.0.0.1:%d", remote_mock1->port);
  manager_vector[0] = utstring_body(addr0);
  UT_string *addr1 = NULL;
  utstring_new(addr1);
  utstring_printf(addr1, "127.0.0.1:%d", remote_mock2->port);
  manager_vector[1] = utstring_body(addr1);
  call_request_transfer(oid, 2, manager_vector, local_mock->state);
  free(manager_vector);
  event_loop_add_timer(local_mock->loop, MANAGER_TIMEOUT * 2, test_done_handler,
                       local_mock->state);
  /* Register the fetch timeout handler. This is normally done when the plasma
   * manager is started. It is needed here so that retries will happen when
   * fetch requests time out. */
  event_loop_add_timer(local_mock->loop, MANAGER_TIMEOUT, fetch_timeout_handler,
                       local_mock->state);
  event_loop_run(local_mock->loop);

  int read_fd = get_client_sock(remote_mock2->read_conn);
  uint8_t *request_data =
      plasma_receive(read_fd, MessageType_PlasmaDataRequest);
  ObjectID oid2;
  char *address;
  int port;
  plasma_read_DataRequest(request_data, &oid2, &address, &port);
  free(address);
  ASSERT(ObjectID_equal(oid, oid2));
  /* Clean up. */
  utstring_free(addr0);
  utstring_free(addr1);
  free(request_data);
  destroy_plasma_mock(remote_mock2);
  destroy_plasma_mock(remote_mock1);
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
  plasma_mock *local_mock = init_plasma_mock(NULL);
  plasma_mock *remote_mock = init_plasma_mock(local_mock);
  /* Create a mock object buffer to transfer. */
  const char *data = "Hello world!";
  const int data_size = strlen(data) + 1;
  const int metadata_size = 0;
  plasma_request_buffer remote_buf;
  remote_buf.type = MessageType_PlasmaDataReply;
  remote_buf.object_id = oid;
  remote_buf.data = (uint8_t *) data;
  remote_buf.data_size = data_size;
  remote_buf.metadata = (uint8_t *) data + data_size;
  remote_buf.metadata_size = metadata_size;
  plasma_request_buffer local_buf;
  local_buf.object_id = oid;
  local_buf.data_size = data_size;
  local_buf.metadata_size = metadata_size;
  local_buf.data = (uint8_t *) malloc(data_size);
  /* The test:
   * - Write the object data from the remote manager to the local.
   * - Read the object data on the local manager.
   * - Check that the data matches.
   */
  write_object_chunk(remote_mock->write_conn, &remote_buf);
  /* Wait until the data is ready to be read. */
  wait_for_pollin(get_client_sock(remote_mock->read_conn));
  /* Read the data. */
  int done = read_object_chunk(remote_mock->read_conn, &local_buf);
  ASSERT(done);
  ASSERT_EQ(memcmp(remote_buf.data, local_buf.data, data_size), 0);
  /* Clean up. */
  free(local_buf.data);
  destroy_plasma_mock(remote_mock);
  destroy_plasma_mock(local_mock);
  PASS();
}

TEST object_notifications_test(void) {
  plasma_mock *local_mock = init_plasma_mock(NULL);
  /* Open a non-blocking socket pair to mock the object notifications from the
   * plasma store. */
  int fd[2];
  socketpair(AF_UNIX, SOCK_STREAM, 0, fd);
  int flags = fcntl(fd[1], F_GETFL, 0);
  CHECK(fcntl(fd[1], F_SETFL, flags | O_NONBLOCK) == 0);

  ObjectID oid = globally_unique_id();
  ObjectInfo info = {.obj_id = oid,
                     .data_size = 10,
                     .metadata_size = 1,
                     .create_time = 0,
                     .construct_duration = 0,
                     .digest = {0},
                     .is_deletion = false};

  /* Check that the object is not local at first. */
  bool is_local = is_object_local(local_mock->state, oid);
  ASSERT(!is_local);

  /* Check that the object is local after receiving an object notification. */
  send(fd[1], (char const *) &info, sizeof(info), 0);
  process_object_notification(local_mock->loop, fd[0], local_mock->state, 0);
  is_local = is_object_local(local_mock->state, oid);
  ASSERT(is_local);

  /* Check that the object is not local after receiving a notification about
   * the object deletion. */
  info.is_deletion = true;
  send(fd[1], (char const *) &info, sizeof(info), 0);
  process_object_notification(local_mock->loop, fd[0], local_mock->state, 0);
  is_local = is_object_local(local_mock->state, oid);
  ASSERT(!is_local);

  /* Clean up. */
  close(fd[0]);
  close(fd[1]);
  destroy_plasma_mock(local_mock);
  PASS();
}

SUITE(plasma_manager_tests) {
  memset(&oid, 1, sizeof(oid));
  RUN_TEST(request_transfer_test);
  RUN_TEST(request_transfer_retry_test);
  RUN_TEST(read_write_object_chunk_test);
  RUN_TEST(object_notifications_test);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
  GREATEST_MAIN_BEGIN();
  RUN_SUITE(plasma_manager_tests);
  GREATEST_MAIN_END();
}
