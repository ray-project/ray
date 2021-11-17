// https://github.com/uber/cadence-java-samples/tree/master/src/main/java/com/uber/cadence/samples/bookingsaga
public class TripBookingWorkflowImpl implements TripBookingWorkflow {

  private final ActivityOptions options =
      new ActivityOptions.Builder().setScheduleToCloseTimeout(Duration.ofHours(1)).build();
  private final TripBookingActivities activities =
      Workflow.newActivityStub(TripBookingActivities.class, options);

  @Override
  public void bookTrip(String name) {
    Saga.Options sagaOptions = new Saga.Options.Builder().setParallelCompensation(true).build();
    Saga saga = new Saga(sagaOptions);
    try {
      String carReservationID = activities.reserveCar(name);
      saga.addCompensation(activities::cancelCar, carReservationID, name);

      String hotelReservationID = activities.bookHotel(name);
      saga.addCompensation(activities::cancelHotel, hotelReservationID, name);

      String flightReservationID = activities.bookFlight(name);
      saga.addCompensation(activities::cancelFlight, flightReservationID, name);
    } catch (ActivityException e) {
      saga.compensate();
      throw e;
    }
  }
}
