import ray
import uuid
from ray import workflow
ray.init(namespace="x")
workflow.init()

book_flight_ok = False

@workflow.step
def book_flight(flight_id):
    if book_flight_ok:
        print(f"Book flight successfully {flight_id}")
        return flight_id
    else:
        print(f"Book flight failed {flight_id}")
        raise RuntimeError()


@workflow.step
def book_hotel(hotel_id):
    print("Book hotel successfully {hotel_id}")
    return hotel_id

@workflow.step
def cancel_all(*args):
    print(f"Canceling {args}")

@workflow.step
def book_trip():
   id1 = uuid.uuid4()
   id2 = uuid.uuid4()
   try:
       r1 = book_flight.step(id1)
       r2 = book_hotel.step(id2)
       flight, hotel = workflow.get([r1, r2])
   except RuntimeError as e:
       workflow.get(cancel_all.step(id1, id2))
       # has to raise with e right now due to unthrow bug
       raise e
   return (flight, hotel)

try:
    print(">>>> Book successfully", book_trip.step().run())
except Exception as e:
    print(">>>> FAILED to book", e)

book_flight_ok = True
try:
    print(">>>> Book successfully", book_trip.step().run())
except Exception as e:
    print(">>>> FAILED", e)
