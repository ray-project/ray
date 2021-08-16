from typing import List, Tuple

from ray import workflow


# Generate an idempotency token (this is an extension to the cadence example).
@workflow.step
def generate_request_id():
    import uuid
    return uuid.uuid4().hex


@workflow.step
def book_car(request_id: str) -> str:
    car_reservation_id = ...  # RPC to book the car
    return car_reservation_id


@workflow.step
def cancel_car(request_id: str) -> None:
    ...  # cancel the car booking


@workflow.step
def book_hotel(request_id: str, *deps) -> str:
    hotel_reservation_id = ...  # RPC to book the hotel
    return hotel_reservation_id


@workflow.step
def cancel_hotel(request_id: str) -> None:
    ...  # cancel the hotel booking


@workflow.step
def book_flight(request_id: str, *deps) -> str:
    flight_reservation_id = ...  # RPC to book the flight
    return flight_reservation_id


@workflow.step
def cancel_flight(request_id: str) -> None:
    ...  # cancel the flight booking


@workflow.step
def book_all(car_req_id: str, hotel_req_id: str, flight_req_id: str) -> str:
    car_res_id = book_car.step(car_req_id)
    hotel_res_id = book_hotel.step(hotel_req_id, car_res_id)
    flight_res_id = book_flight.step(hotel_req_id, hotel_res_id)

    @workflow.step
    def concat(*ids: List[str]) -> str:
        return ", ".join(ids)

    return concat.step(car_res_id, hotel_res_id, flight_res_id)


@workflow.step
def handle_errors(car_req_id: str, hotel_req_id: str, flight_req_id: str,
                  final_result: Tuple[str, Exception]) -> str:
    result, error = final_result

    @workflow.step
    def wait_all(*deps) -> None:
        pass

    if error:
        return wait_all.step(
            cancel_car.step(car_req_id), cancel_hotel.step(hotel_req_id),
            cancel_flight.step(flight_req_id))
    else:
        return result


if __name__ == "__main__":
    workflow.init()

    car_req_id = generate_request_id.step()
    hotel_req_id = generate_request_id.step()
    flight_req_id = generate_request_id.step()

    saga_result = book_all.options(catch_exceptions=True) \
        .step(car_req_id, hotel_req_id, flight_req_id)

    final_result = handle_errors.step(car_req_id, hotel_req_id, flight_req_id,
                                      saga_result)
    print(final_result.run())
