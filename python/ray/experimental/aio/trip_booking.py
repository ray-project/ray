import logging

import asyncio
import time
import uuid

import ray
import ray.experimental.aio as rayaio

ray.init()


@ray.remote
def cancel(reserv_id: str) -> None:
    time.sleep(2)


@ray.remote
def book_car(request_id: str) -> str:
    time.sleep(2)
    book_id = f"book_car:{request_id}"
    print(f"Booked: {book_id}")
    return book_id


@ray.remote
def book_hotel(request_id: str, *deps) -> str:
    time.sleep(2)
    book_id = f"book_hotel:{request_id}"
    print(f"Booked: {book_id}")
    return book_id


@ray.remote
def book_flight(request_id: str, *deps) -> str:
    time.sleep(1)
    book_id = f"book_flight:{request_id}"
    print(f"Booked: {book_id}")
    return book_id


# This async function defines the workflow.
async def book_all(request_id: str):
    car_reserv_id, hotel_reserv_id, flight_reserv_id = None, None, None
    # Adding a workflow ultility may help with the pattern of undo.
    undo = []
    try:
        # When starting to await, the book_all() coroutine is checkpointed.
        car_reservation_id = await book_car.remote(request_id)
        # When the result is available, the coroutine is checkpointed again.

        undo.append(lambda : cancel(car_reservation_id))

        # Checkpointing here automatically saves the progress of this coroutine,
        # and stack variables like car_reservation_id, undo.
        hotel_reservation_id = await book_hotel.remote(request_id)
        undo.append(lambda : cancel(hotel_reservation_id))

        flight_reservation_id = await book_flight.remote(request_id)
        undo.append(lambda : cancel(flight_reservation_id))
    except Exception:
        # Currently we cannot checkpoint inside an except block, because the
        # traceback object cannot be pickled, which is on the stack.
        failed = True
    if failed:
        # Assume cancellations will not fail / throw exceptions.
        print(f"Canceling finished tasks ...")
        for callback in undo:
            await callback.remote()
        return
    print(f"Booking finished: {car_reservation_id} {hotel_reservation_id} "
          f"{flight_reservation_id}")

key = uuid.uuid4().hex
rayaio.demo_workflow(key, book_all(key))
