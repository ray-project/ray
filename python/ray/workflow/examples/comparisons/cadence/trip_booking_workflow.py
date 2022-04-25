from typing import List, Tuple, Optional

import ray
from ray import workflow


# Mock method to make requests to an external service.
def make_request(*args) -> None:
    return "result"


# Generate an idempotency token (this is an extension to the cadence example).
@workflow.step
def generate_request_id():
    import uuid

    return uuid.uuid4().hex


@ray.remote
def book_car(request_id: str) -> str:
    car_reservation_id = make_request("book_car", request_id)
    return car_reservation_id


@ray.remote
def book_hotel(request_id: str, *deps) -> str:
    hotel_reservation_id = make_request("book_hotel", request_id)
    return hotel_reservation_id


@ray.remote
def book_flight(request_id: str, *deps) -> str:
    flight_reservation_id = make_request("book_flight", request_id)
    return flight_reservation_id


@workflow.step
def book_all(car_req_id: str, hotel_req_id: str, flight_req_id: str) -> str:
    car_res_id = book_car.bind(car_req_id)
    hotel_res_id = book_hotel.bind(hotel_req_id, car_res_id)
    flight_res_id = book_flight.bind(hotel_req_id, hotel_res_id)

    @ray.remote
    def concat(*ids: List[str]) -> str:
        return ", ".join(ids)

    return workflow.continuation(concat.bind(car_res_id, hotel_res_id, flight_res_id))


@workflow.step
def handle_errors(
    car_req_id: str,
    hotel_req_id: str,
    flight_req_id: str,
    final_result: Tuple[Optional[str], Optional[Exception]],
) -> str:
    result, error = final_result

    @ray.remote
    def wait_all(*deps) -> None:
        pass

    @ray.remote
    def cancel(request_id: str) -> None:
        make_request("cancel", request_id)

    if error:
        return workflow.continuation(
            wait_all.bind(
                cancel.bind(car_req_id),
                cancel.bind(hotel_req_id),
                cancel.bind(flight_req_id),
            )
        )
    else:
        return result


if __name__ == "__main__":
    workflow.init()
    car_req_id = generate_request_id.step()
    hotel_req_id = generate_request_id.step()
    flight_req_id = generate_request_id.step()
    # TODO(ekl) we could create a Saga helper function that automates this
    # pattern of compensation workflows.
    saga_result = book_all.options(catch_exceptions=True).step(
        car_req_id, hotel_req_id, flight_req_id
    )
    final_result = handle_errors.step(
        car_req_id, hotel_req_id, flight_req_id, saga_result
    )
    print(final_result.run())
