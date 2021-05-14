"""
Simulator for the Moab plate+ball balancing device.
"""
__author__ = "Mike Estee"
__copyright__ = "Copyright 2020, Microsoft Corp."

# pyright: strict

import math
import random
from typing import Dict, Tuple, cast

import numpy as np
from pyrr import Quaternion, Vector3, matrix44, quaternion, ray, vector
from pyrr.geometric_tests import ray_intersect_plane
from pyrr.plane import create_from_position

# Some type aliases for clarity
Plane = np.ndarray
Ray = np.ndarray

DEFAULT_TIME_DELTA = 0.045  # s, 45ms
DEFAULT_GRAVITY = 9.81  # m/s^2, Earth: there's no place like it.

DEFAULT_BALL_RADIUS = 0.02  # m, Ping-Pong ball: 20mm
DEFAULT_BALL_SHELL = 0.0002  # m, Ping-Pong ball: 0.2mm
DEFAULT_BALL_MASS = 0.0027  # kg, Ping-Pong ball: 2.7g

DEFAULT_OBSTACLE_RADIUS = 0.0  # m, if radius is zero, obstacle is disabled
DEFAULT_OBSTACLE_X = 0.03  # m, arbitrarily chosen
DEFAULT_OBSTACLE_Y = 0.03  # m, arbitrarily chosen

DEFAULT_PLATE_RADIUS = 0.225 / 2.0  # m, Moab: 225mm dia
PLATE_ORIGIN_TO_SURFACE_OFFSET = (
    0.009  # 9mm offset from plate rot origin to plate surface
)

# plate limits
PLATE_HEIGHT_MAX = 0.040  # m, Moab: 40mm
DEFAULT_PLATE_HEIGHT = PLATE_HEIGHT_MAX / 2.0
DEFAULT_PLATE_ANGLE_LIMIT = math.radians(44.0 * 0.5)  # rad, 1/2 full range
DEFAULT_PLATE_Z_LIMIT = PLATE_HEIGHT_MAX / 2.0  # m, +/- limit from center Z pos

# default ball Z position
DEFAULT_BALL_Z_POSITION = (
    DEFAULT_PLATE_HEIGHT + PLATE_ORIGIN_TO_SURFACE_OFFSET + DEFAULT_BALL_RADIUS
)

PLATE_MAX_Z_VELOCITY = 1.0  # m/s
PLATE_Z_ACCEL = 10.0  # m/s^2

# Moab measured velocity at 15deg in 3/60ths, or 300deg/s
DEFAULT_PLATE_MAX_ANGULAR_VELOCITY = (60.0 / 3.0) * math.radians(15)  # rad/s

# Set acceleration to get the plate up to velocity in 1/100th of a sec
DEFAULT_PLATE_ANGULAR_ACCEL = (
    100.0 / 1.0
) * DEFAULT_PLATE_MAX_ANGULAR_VELOCITY  # rad/s^2

# useful constants
X_AXIS = np.array([1.0, 0.0, 0.0])
Y_AXIS = np.array([0.0, 1.0, 0.0])
Z_AXIS = np.array([0.0, 0.0, 1.0])

# Sensor Actuator Noises
DEFAULT_PLATE_NOISE = 0.0  # noise added to plate_theta_* (rad)
DEFAULT_BALL_NOISE = 0.0  # noise added to estimated_* ball location (m)
DEFAULT_JITTER = 0.0  # jitter added to step_time (s)


def clamp(val: float, min_val: float, max_val: float):
    return min(max_val, max(min_val, val))


class MoabModel:
    def __init__(self):
        self.reset()

    def reset(self):
        """
        Resets the model to known default state.

        If further changes are applied after reseting, the caller should call:
            model.update_plate(True)
            model.update_ball(True)
        """
        # general config
        self.time_delta = DEFAULT_TIME_DELTA
        self.jitter = DEFAULT_JITTER
        self.step_time = self.time_delta
        self.elapsed_time = 0.0
        self.gravity = DEFAULT_GRAVITY

        # plate config
        self.plate_noise = DEFAULT_PLATE_NOISE
        self.plate_radius = DEFAULT_PLATE_RADIUS
        self.plate_theta_limit = DEFAULT_PLATE_ANGLE_LIMIT
        self.plate_theta_vel_limit = DEFAULT_PLATE_MAX_ANGULAR_VELOCITY
        self.plate_theta_acc = DEFAULT_PLATE_ANGULAR_ACCEL
        self.plate_z_limit = DEFAULT_PLATE_Z_LIMIT

        # ball config
        self.ball_noise = DEFAULT_BALL_NOISE
        self.ball_mass = DEFAULT_BALL_MASS
        self.ball_radius = DEFAULT_BALL_RADIUS
        self.ball_shell = DEFAULT_BALL_SHELL

        # control input (unitless) [-1..1]
        self.pitch = 0.0
        self.roll = 0.0
        self.height_z = 0.0

        # plate state
        self.plate_theta_x = 0.0
        self.plate_theta_y = 0.0
        self.plate = Vector3([0.0, 0.0, DEFAULT_PLATE_HEIGHT])

        self.plate_theta_vel_x = 0.0
        self.plate_theta_vel_y = 0.0
        self.plate_vel_z = 0.0

        # ball state
        self.ball = Vector3([0.0, 0.0, DEFAULT_BALL_Z_POSITION])
        self.ball_vel = Vector3([0.0, 0.0, 0.0])
        self.ball_qat = Quaternion([0.0, 0.0, 0.0, 1.0])
        self.ball_on_plate = Vector3(
            [0.0, 0.0, PLATE_ORIGIN_TO_SURFACE_OFFSET + DEFAULT_BALL_RADIUS]
        )

        # current target
        self.target_x = 0.0
        self.target_y = 0.0

        # current obstacle
        self.obstacle_distance = 0.0
        self.obstacle_direction = 0.0
        self.obstacle_radius = 0.0
        self.obstacle_x = 0.0
        self.obstacle_y = 0.0

        # camera observed estimated metrics
        self.estimated_x = 0.0
        self.estimated_y = 0.0
        self.estimated_vel_x = 0.0
        self.estimated_vel_y = 0.0
        self.estimated_radius = self.ball_radius

        # target relative polar coords/vel
        self.estimated_speed = 0.0
        self.estimated_direction = 0.0
        self.estimated_distance = 0.0

        self.prev_estimated_x = 0.0
        self.prev_estimated_y = 0.0

        # meta
        self.iteration_count = 0

        # now that the base state has been set, run an update
        # to make sure the all variables are internally constistent
        self.update_plate(True)
        self.update_ball(True)

    def halted(self) -> bool:
        """
        Returns True if the ball is off the plate.
        """
        # ball.z relative to plate
        zpos = self.ball.z - (
            self.plate.z + self.ball_radius + PLATE_ORIGIN_TO_SURFACE_OFFSET
        )

        # ball distance from ball position on plate at origin
        distance_to_center = math.sqrt(
            math.pow(self.ball.x, 2.0)
            + math.pow(self.ball.y, 2.0)
            + math.pow(zpos, 2.0)
        )

        return distance_to_center > self.plate_radius

    def step(self):
        """
        Single step the simulation.

        The current actions will be applied, and the model evaluated.
        All state variables will be updated.
        """
        self.step_time = self.time_delta + MoabModel.random_noise(self.jitter)
        self.elapsed_time += self.step_time

        self.update_plate(False)
        self.update_ball(False)

        # update meta
        self.iteration_count += 1

    # returns a noise value in the range [-scalar .. scalar] with a gaussian distribution
    @staticmethod
    def random_noise(scalar: float) -> float:
        return scalar * clamp(
            random.gauss(mu=0, sigma=0.333), -1, 1
        )  # mean zero gauss with sigma = ~sqrt(scalar)/3

    @staticmethod
    def accel_param(
        q: float, dest: float, vel: float, acc: float, max_vel: float, delta_t: float
    ) -> Tuple[float, float]:
        """
        perform a linear acceleration of variable towards a destination
        with a hard stop at the destination. returns the position and velocity
        after delta_t has elapsed.

        q:      initial position
        dest:   target destination
        vel:    current velocity
        acc:    acceleration constant
        max_vel: maximum velocity
        delta_t: time delta

        returns: (final_position, final_velocity)
        """
        # direction of accel
        dir = 0.0
        if q < dest:
            dir = 1.0
        if q > dest:
            dir = -1.0

        # calculate the change in velocity and position
        acc = acc * dir * delta_t
        vel_end = clamp(vel + acc * delta_t, -max_vel, max_vel)
        vel_avg = (vel + vel_end) * 0.5
        delta = vel_avg * delta_t
        vel = vel_end

        # moving towards the dest?
        if (dir > 0 and q < dest and q + delta < dest) or (
            dir < 0 and q > dest and q + delta > dest
        ):
            q = q + delta

        # stop at dest
        else:
            q = dest
            vel = 0

        return (q, vel)

    @staticmethod
    def heading_to_point(
        start_x: float,
        start_y: float,
        vel_x: float,
        vel_y: float,
        point_x: float,
        point_y: float,
    ):
        """
        Return a heading, in 2D RH coordinate system.
        x,y:                the current position of the object
        vel_x, vel_y:       the current velocity vector of motion for the object
        point_x, point_y:   the destination point to head towards

        returns: offset angle in radians in the range [-pi .. pi]
        where:
            0.0:                object is moving directly towards the point
            [-pi .. <0]:   object is moving to the "right" of the point
            [>0 .. -pi]:   object is moving to the "left" of the point
            [-pi, pi]: object is moving directly away from the point
        """
        # vector to point
        dx = point_x - start_x
        dy = point_y - start_y

        # if the ball is already at the target location or
        # is not moving, return a heading of 0 so we don't
        # attempt to normalize a zero-length vector
        if dx == 0 and dy == 0:
            return 0
        if vel_x == 0 and vel_y == 0:
            return 0

        # vectors and lengths
        u = vector.normalize([dx, dy, 0.0])
        v = vector.normalize([vel_x, vel_y, 0.0])
        ul = vector.length(u)
        vl = vector.length(v)

        # no velocity? already on the target?
        angle = 0.0
        if (ul != 0.0) and (vl != 0.0):
            # angle between vectors
            uv_dot = vector.dot(u, v)

            # signed angle
            x = u[0]
            y = u[1]
            angle = math.atan2(vector.dot([-y, x, 0.0], v), uv_dot)
            if math.isnan(angle):
                angle = 0.0
        return angle

    @staticmethod
    def distance_to_point(x: float, y: float, point_x: float, point_y: float) -> float:
        """
        Return the distance between two 2D points.
        """
        dx = point_x - x
        dy = point_y - y
        return math.sqrt((dx ** 2.0) + (dy ** 2.0))

    # convert X/Y theta components into a Z-Up RH plane normal
    def _plate_nor(self) -> Vector3:
        x_rot = matrix44.create_from_axis_rotation(
            axis=X_AXIS, theta=self.plate_theta_x
        )
        y_rot = matrix44.create_from_axis_rotation(
            axis=Y_AXIS, theta=self.plate_theta_y
        )

        # pitch then roll
        nor = matrix44.apply_to_vector(mat=x_rot, vec=Z_AXIS)
        nor = matrix44.apply_to_vector(mat=y_rot, vec=nor)
        nor = vector.normalize(nor)

        return Vector3(nor)

    def update_plate(self, plate_reset: bool = False):
        # Find the target xth,yth & zpos
        # convert xy[-1..1] to zx[-self.plate_theta_limit .. self.plate_theta_limit]
        # convert z[-1..1] to [PLATE_HEIGHT_MAX/2 - self.plate_z_limit .. PLATE_HEIGHT_MAX/2 + self.plate_z_limit]
        theta_x_target = self.plate_theta_limit * self.pitch  # pitch around X axis
        theta_y_target = self.plate_theta_limit * self.roll  # roll around Y axis
        z_target = (self.height_z * self.plate_z_limit) + PLATE_HEIGHT_MAX / 2.0

        # quantize target positions to whole degree increments
        # the Moab hardware can only command by whole degrees
        theta_y_target = math.radians(round(math.degrees(theta_y_target)))
        theta_x_target = math.radians(round(math.degrees(theta_x_target)))

        # get the current xth,yth & zpos
        theta_x, theta_y = self.plate_theta_x, self.plate_theta_y
        z_pos = self.plate.z

        # on reset, bypass the motion equations
        if plate_reset:
            theta_x = theta_x_target
            theta_y = theta_y_target
            z_pos = z_target

        # smooth transition to target based on accel and velocity limits
        else:
            theta_x, self.plate_theta_vel_x = MoabModel.accel_param(
                theta_x,
                theta_x_target,
                self.plate_theta_vel_x,
                self.plate_theta_acc,
                self.plate_theta_vel_limit,
                self.step_time,
            )
            theta_y, self.plate_theta_vel_y = MoabModel.accel_param(
                theta_y,
                theta_y_target,
                self.plate_theta_vel_y,
                self.plate_theta_acc,
                self.plate_theta_vel_limit,
                self.step_time,
            )
            z_pos, self.plate_vel_z = MoabModel.accel_param(
                z_pos,
                z_target,
                self.plate_vel_z,
                PLATE_Z_ACCEL,
                PLATE_MAX_Z_VELOCITY,
                self.step_time,
            )

            # add noise to the plate positions
            theta_x += MoabModel.random_noise(self.plate_noise)
            theta_y += MoabModel.random_noise(self.plate_noise)

        # clamp to range limits
        theta_x = clamp(theta_x, -self.plate_theta_limit, self.plate_theta_limit)
        theta_y = clamp(theta_y, -self.plate_theta_limit, self.plate_theta_limit)
        z_pos = clamp(
            z_pos,
            PLATE_HEIGHT_MAX / 2.0 - self.plate_z_limit,
            PLATE_HEIGHT_MAX / 2.0 + self.plate_z_limit,
        )

        # Now convert back to plane parameters
        self.plate_theta_x = theta_x
        self.plate_theta_y = theta_y
        self.plate.z = z_pos

    # ball intertia with radius and hollow radius
    # I = 2/5 * m * ((r^5 - h^5) / (r^3 - h^3))
    def _ball_inertia(self):
        hollow_radius = self.ball_radius - self.ball_shell
        return (
            2.0
            / 5.0
            * self.ball_mass
            * (
                (math.pow(self.ball_radius, 5.0) - math.pow(hollow_radius, 5.0))
                / (math.pow(self.ball_radius, 3.0) - math.pow(hollow_radius, 3.0))
            )
        )

    def _camera_pos(self) -> Vector3:
        """ camera origin (lens center) in world space """
        return Vector3([0.0, 0.0, -0.052])

    def _update_estimated_ball(self, ball: Vector3):
        """
        Ray trace the ball position and an edge of the ball back to the camera
        origin and use the collision points with the tilted plate to estimate
        what a camera might perceive the ball position and size to be.
        """
        # contact ray from camera to plate
        camera = self._camera_pos()
        displacement = camera - self.ball
        displacement_radius = camera - (self.ball + Vector3([self.ball_radius, 0, 0]))

        ball_ray = ray.create(camera, displacement)
        ball_radius_ray = ray.create(camera, displacement_radius)

        surface_plane = self._surface_plane()

        contact = Vector3(ray_intersect_plane(ball_ray, surface_plane, False))
        radius_contact = Vector3(
            ray_intersect_plane(ball_radius_ray, surface_plane, False)
        )

        x, y = contact.x, contact.y
        r = math.fabs(contact.x - radius_contact.x)

        # add the noise in
        self.estimated_x = x + MoabModel.random_noise(self.ball_noise)
        self.estimated_y = y + MoabModel.random_noise(self.ball_noise)
        self.estimated_radius = r + MoabModel.random_noise(self.ball_noise)

        # Use n-1 states to calculate an estimated velocity.
        self.estimated_vel_x = (
            self.estimated_x - self.prev_estimated_x
        ) / self.step_time
        self.estimated_vel_y = (
            self.estimated_y - self.prev_estimated_y
        ) / self.step_time

        # distance to target
        self.estimated_distance = MoabModel.distance_to_point(
            self.estimated_x, self.estimated_y, self.target_x, self.target_y
        )

        # update the derived states
        self.estimated_speed = cast(
            float, vector.length([self.ball_vel.x, self.ball_vel.y, self.ball_vel.z])
        )

        self.estimated_direction = MoabModel.heading_to_point(
            self.estimated_x,
            self.estimated_y,
            self.estimated_vel_x,
            self.estimated_vel_y,
            self.target_x,
            self.target_y,
        )

        # update for next time
        self.prev_estimated_x = self.estimated_x
        self.prev_estimated_y = self.estimated_y

        # update ball position in plate origin coordinates, and obstacle distance and direction
        self.ball_on_plate = self.world_to_plate(self.ball.x, self.ball.y, self.ball.z)
        self.obstacle_distance = self._get_obstacle_distance()
        self.obstacle_direction = MoabModel.heading_to_point(
            self.ball.x,
            self.ball.y,
            self.ball_vel.x,
            self.ball_vel.y,
            self.obstacle_x,
            self.obstacle_y,
        )

    def _get_obstacle_distance(self) -> float:
        # Ignore z value, calculate distance between obstacle and ball projection on plate
        distance_between_centers = math.sqrt(
            math.pow(self.ball_on_plate.x - self.obstacle_x, 2.0)
            + math.pow(self.ball_on_plate.y - self.obstacle_y, 2.0)
        )

        # Negative distance to obstacle means the ball and obstacle are  overlapping
        return distance_between_centers - self.ball_radius - self.obstacle_radius

    def _surface_plane(self) -> Plane:
        """
        Return the surface plane of the plate
        """
        plate_surface = np.array(
            [self.plate.x, self.plate.y, self.plate.z + PLATE_ORIGIN_TO_SURFACE_OFFSET]
        )
        return create_from_position(plate_surface, self._plate_nor())

    def _motion_for_time(
        self, u: Vector3, a: Vector3, t: float
    ) -> Tuple[Vector3, Vector3]:
        """
        Equations of motion for displacement and final velocity
        u: initial velocity
        a: acceleration
        d: displacement
        v: final velocity

        d = ut + 1/2at^2
        v = u + at

        returns (d, v)
        """
        d = (u * t) + (0.5 * a * (t ** 2))
        v = u + a * t
        return d, v

    def _update_ball_z(self):
        self.ball.z = (
            self.ball.x * math.sin(-self.plate_theta_y)
            + self.ball.y * math.sin(self.plate_theta_x)
            + self.ball_radius
            + self.plate.z
            + PLATE_ORIGIN_TO_SURFACE_OFFSET
        )

    def _ball_plate_contact(self, step_t: float) -> float:
        # NOTE: the x_theta axis creates motion in the Y-axis, and vice versa
        # x_theta, y_theta = self._xy_theta_from_nor(self.plate_nor.xyz)
        x_theta = self.plate_theta_x
        y_theta = self.plate_theta_y

        # Equations for acceleration on a plate at rest
        # accel = (mass * g * theta) / (mass + inertia / radius^2)
        # (y_theta,x are intentional swapped here.)
        theta = Vector3([y_theta, -x_theta, 0])
        self.ball_acc = (
            theta
            / (self.ball_mass + self._ball_inertia() / (self.ball_radius ** 2))
            * self.ball_mass
            * self.gravity
        )

        # get contact displacement
        disp, vel = self._motion_for_time(self.ball_vel, self.ball_acc, step_t)

        # simplified ball mechanics against a plane
        self.ball.x += disp.x
        self.ball.y += disp.y
        self._update_ball_z()
        self.ball_vel = vel

        # For rotation on plate motion we use infinite friction and
        # perfect ball / plate coupling.
        # Calculate the distance we traveled across the plate during
        # this time slice.
        rot_distance = math.hypot(disp.x, disp.y)
        if rot_distance > 0:
            # Calculate the fraction of the circumference that we traveled
            # (in radians).
            rot_angle = rot_distance / self.ball_radius

            # Create a quaternion that represents the delta rotation for this time period.
            # Note that we translate the (x, y) direction into (y, -x) because we're
            # creating a vector that represents the axis of rotation which is normal
            # to the direction the ball traveled in the x/y plane.
            rot_q = quaternion.normalize(
                np.array(
                    [
                        disp.y / rot_distance * math.sin(rot_angle / 2.0),
                        -disp.x / rot_distance * math.sin(rot_angle / 2.0),
                        0.0,
                        math.cos(rot_angle / 2.0),
                    ]
                )
            )

            old_rot = self.ball_qat.xyzw
            new_rot = quaternion.cross(quat1=old_rot, quat2=rot_q)
            self.ball_qat.xyzw = quaternion.normalize(new_rot)
        return 0.0

    def plate_to_world(self, x: float, y: float, z: float) -> Vector3:
        # rotate
        x_rot = matrix44.create_from_axis_rotation([1.0, 0.0, 0.0], self.plate_theta_x)
        y_rot = matrix44.create_from_axis_rotation([0.0, 1.0, 0.0], self.plate_theta_y)
        vec = matrix44.apply_to_vector(mat=x_rot, vec=[x, y, z])
        vec = matrix44.apply_to_vector(mat=y_rot, vec=vec)

        # translate
        move = matrix44.create_from_translation(
            [self.plate.x, self.plate.y, self.plate.z + PLATE_ORIGIN_TO_SURFACE_OFFSET]
        )
        vec = matrix44.apply_to_vector(mat=move, vec=vec)

        return Vector3(vec)

    def world_to_plate(self, x: float, y: float, z: float) -> Vector3:
        move = matrix44.create_from_translation(
            [
                -self.plate.x,
                -self.plate.y,
                -(self.plate.z + PLATE_ORIGIN_TO_SURFACE_OFFSET),
            ]
        )
        vec = matrix44.apply_to_vector(mat=move, vec=[x, y, z])

        # rotate
        x_rot = matrix44.create_from_axis_rotation([1.0, 0.0, 0.0], -self.plate_theta_x)
        y_rot = matrix44.create_from_axis_rotation([0.0, 1.0, 0.0], -self.plate_theta_y)
        vec = matrix44.apply_to_vector(mat=x_rot, vec=vec)
        vec = matrix44.apply_to_vector(mat=y_rot, vec=vec)

        return Vector3(vec)

    def set_initial_ball(self, x: float, y: float, z: float):
        self.ball.xyz = [x, y, z]
        self._update_ball_z()

        # Set initial observations
        self._update_estimated_ball(self.ball)
        pass

    def update_ball(self, ball_reset: bool = False):
        """
        Update the ball position with the physics model.
        """
        if ball_reset:
            # this just ensures that the ball is on the plate
            self._update_ball_z()
        else:
            self._ball_plate_contact(self.step_time)

        # Finally, lets make some approximations for observations
        self._update_estimated_ball(self.ball)

    def state(self) -> Dict[str, float]:
        # x_theta, y_theta = self._xy_theta_from_nor(self.plate_nor)
        plate_nor = self._plate_nor()

        return dict(
            # reflected input controls
            roll=self.roll,
            pitch=self.pitch,
            height_z=self.height_z,
            # reflected constants
            time_delta=self.time_delta,
            jitter=self.jitter,
            step_time=self.step_time,
            elapsed_time=self.elapsed_time,
            gravity=self.gravity,
            plate_radius=self.plate_radius,
            plate_theta_vel_limit=self.plate_theta_vel_limit,
            plate_theta_acc=self.plate_theta_acc,
            plate_theta_limit=self.plate_theta_limit,
            plate_z_limit=self.plate_z_limit,
            ball_mass=self.ball_mass,
            ball_radius=self.ball_radius,
            ball_shell=self.ball_shell,
            obstacle_radius=self.obstacle_radius,
            obstacle_x=self.obstacle_x,
            obstacle_y=self.obstacle_y,
            target_x=self.target_x,
            target_y=self.target_y,
            # modelled plate metrics
            plate_x=self.plate.x,
            plate_y=self.plate.y,
            plate_z=self.plate.z,
            plate_nor_x=plate_nor.x,
            plate_nor_y=plate_nor.y,
            plate_nor_z=plate_nor.z,
            plate_theta_x=self.plate_theta_x,
            plate_theta_y=self.plate_theta_y,
            plate_theta_vel_x=self.plate_theta_vel_x,
            plate_theta_vel_y=self.plate_theta_vel_y,
            plate_vel_z=self.plate_vel_z,
            # modelled ball metrics
            ball_x=self.ball.x,
            ball_y=self.ball.y,
            ball_z=self.ball.z,
            ball_vel_x=self.ball_vel.x,
            ball_vel_y=self.ball_vel.y,
            ball_vel_z=self.ball_vel.z,
            ball_qat_x=self.ball_qat.x,
            ball_qat_y=self.ball_qat.y,
            ball_qat_z=self.ball_qat.z,
            ball_qat_w=self.ball_qat.w,
            ball_on_plate_x=self.ball_on_plate.x,
            ball_on_plate_y=self.ball_on_plate.y,
            obstacle_distance=self.obstacle_distance,
            obstacle_direction=self.obstacle_direction,
            # modelled camera observations
            estimated_x=self.estimated_x,
            estimated_y=self.estimated_y,
            estimated_radius=self.estimated_radius,
            estimated_vel_x=self.estimated_vel_x,
            estimated_vel_y=self.estimated_vel_y,
            # modelled positions and velocities
            estimated_speed=self.estimated_speed,
            estimated_direction=self.estimated_direction,
            estimated_distance=self.estimated_distance,
            ball_noise=self.ball_noise,
            plate_noise=self.plate_noise,
            # meta vars
            ball_fell_off=1 if self.halted() else 0,
            iteration_count=self.iteration_count,
        )
