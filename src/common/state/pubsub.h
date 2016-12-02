#ifndef TRANSFER_PUBSUB_H
#define TRANSFER_PUBSUB_H

#include "table.h"

typedef void (*pubsub_transfer_callback)(object_id object_id,
                                         int source,
                                         int destination,
                                         void *user_context);

void pubsub_request_transfer(db_handle *db,
                             object_id object_id,
                             int source,
                             int destionation,
                             retry_info *retry,
                             pubsub_transfer_callback done_callback,
                             void *user_context);

typedef void (*pubsub_subscribe_transfer_callback)(object_id object_id,
                                                   void *user_context);

void pubsub_subscribe_transfer(db_handle *db,
                               int source,
                               pubsub_subscribe_transfer_callback subscribe_callback,
                               retry_info *retry,
                               pubsub_transfer_callback done_callback,
                               void *user_context);

typedef struct {
  object_id object_id;
  int source;
  int destination;
  pubsub_transfer_callback subscribe_callback;
} transfer_data;

#endif /* TRANSFER_PUBSUB_H */
