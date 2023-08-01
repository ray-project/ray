obj_ref_generator = replica.new_protocol.remote()
first_ref = next(obj_ref_generator) # give us queue length
queue_len = await first_ref # on both this and the other replica we try
replica1.set_assignment.remote(request_id, request_args)
replica2.cancel_assignment.remote(request_id)

# If handle is streaming:
    return obj_ref_generator
# else:
    return next(obj_ref_generator) # result is obj_ref
