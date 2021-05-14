"""
Unit tests for Moab physics model
"""
__copyright__ = "Copyright 2020, Microsoft Corp."

# pyright: strict

import math

from pyrr import Vector3, vector

from moab_model import MoabModel

model = MoabModel()


def run_for_duration(duration: float):
    """ Runs the model without actions for a duration """
    # sync the plate position with commanded position
    model.update_plate(True)

    # run for duration
    elapsed = 0.0  # type: float
    while elapsed < duration:
        model.step()
        elapsed += model.step_time


# basic test for heading
def test_heading():
    heading = MoabModel.heading_to_point(0.0, 0.0, 1.0, 0.0, 1.0, 0.0)
    assert heading == 0.0, "Expected heading to be 0.0 while moving towards point"

    heading = MoabModel.heading_to_point(0.0, 0.0, -1.0, 0.0, 1.0, 0.0)
    assert (
        heading == math.pi
    ), "Expected heading to be math.pi while moving away from point"

    heading = MoabModel.heading_to_point(0.0, 0.0, 0.0, -1.0, 1.0, 0.0)
    assert (
        heading == -math.pi / 2
    ), "Expected heading to be negative while moving to right of point"

    heading = MoabModel.heading_to_point(0.0, 0.0, 0.0, 1.0, 1.0, 0.0)
    assert (
        heading == math.pi / 2
    ), "Expected heading to be positive while moving to left of point"


"""
Roll tests.

The start the ball at the center of the plate and then
command the plate to a tilt position and test the ball
position after 1 second. The ball should be in a known location.

This tests for:
- sign inversions on the axis
- gravity or other mass related constants being off
- differences in axis behavior
"""

# constants for roll tests
ROLL_DIST = 0.1105
ROLL_DIST_LO = ROLL_DIST - 0.0001
ROLL_DIST_HI = ROLL_DIST + 0.0001
TILT = 0.1

# disable the noise for the unit tests
def model_init(model: MoabModel):
    model.plate_noise = 0.0
    model.ball_noise = 0.0
    model.jitter = 0.0


def test_roll_right():
    model.reset()
    model_init(model)
    model.roll = TILT
    run_for_duration(1.0)
    q = model.ball.x
    assert q > ROLL_DIST_LO and q < ROLL_DIST_HI


def test_roll_left():
    model.reset()
    model_init(model)
    model.roll = -TILT
    run_for_duration(1.0)
    q = model.ball.x
    assert -q > ROLL_DIST_LO and -q < ROLL_DIST_HI


def test_roll_back():
    model.reset()
    model_init(model)
    model.pitch = -TILT
    run_for_duration(1.0)
    q = model.ball.y
    assert q > ROLL_DIST_LO and q < ROLL_DIST_HI


def test_roll_front():
    model.reset()
    model_init(model)
    model.pitch = TILT
    run_for_duration(1.0)
    q = model.ball.y
    assert -q > ROLL_DIST_LO and -q < ROLL_DIST_HI


"""
Tilt tests.

These test that the command pitch/roll values move the plate to the limits.
"""


def test_pitch():
    model.reset()
    model_init(model)
    model.pitch = 1.0
    run_for_duration(1.0)
    assert model.plate_theta_x == model.plate_theta_limit

    model.reset()
    model_init(model)
    model.pitch = -1.0
    run_for_duration(1.0)
    assert model.plate_theta_x == -model.plate_theta_limit


def test_roll():
    model.reset()
    model_init(model)
    model.roll = 1.0
    run_for_duration(1.0)
    assert model.plate_theta_y == model.plate_theta_limit

    model.reset()
    model_init(model)
    model.roll = -1.0
    run_for_duration(1.0)
    assert model.plate_theta_y == -model.plate_theta_limit


"""
Coordinate origin transform tests.

These test that transforming between coordinate systems and back yields the original coordinates.
"""
test_vector = Vector3([0.015, 0.020, 0.030])
TOLERANCE = 0.0000000001


def test_world_to_plate_to_world():
    vec_plate = model.world_to_plate(test_vector.x, test_vector.y, test_vector.z)
    result = model.plate_to_world(vec_plate.x, vec_plate.y, vec_plate.z)
    delta = vector.length(result - test_vector)
    assert delta < TOLERANCE


def test_plate_to_world_to_plate():
    vec_world = model.plate_to_world(test_vector.x, test_vector.y, test_vector.z)
    result = model.world_to_plate(vec_world.x, vec_world.y, vec_world.z)
    delta = vector.length(result - test_vector)
    assert delta < TOLERANCE


if __name__ == "__main__":
    test_heading()

    test_roll_right()
    test_roll_left()
    test_roll_front()
    test_roll_back()

    test_roll()
    test_pitch()

    test_world_to_plate_to_world()
    test_plate_to_world_to_plate()
