<mujoco model="reacher">
	<compiler angle="radian" inertiafromgeom="true"/>
	<default>
		<joint armature="1" damping="1" limited="true"/>
		<geom contype="0" friction="1 0.1 0.1" rgba="0.7 0.7 0 1"/>
	</default>
	<option gravity="0 0 -9.81" integrator="RK4" timestep="0.01"/>
	<worldbody>
		<!-- Arena -->
		<geom conaffinity="0" contype="0" name="ground" pos="0 0 0" rgba="0.9 0.9 0.9 1" size="1 1 10" type="plane"/>
		<geom conaffinity="0" fromto="-.3 -.3 .01 .3 -.3 .01" name="sideS" rgba="0.9 0.4 0.6 1" size=".02" type="capsule"/>
		<geom conaffinity="0" fromto=" .3 -.3 .01 .3  .3 .01" name="sideE" rgba="0.9 0.4 0.6 1" size=".02" type="capsule"/>
		<geom conaffinity="0" fromto="-.3  .3 .01 .3  .3 .01" name="sideN" rgba="0.9 0.4 0.6 1" size=".02" type="capsule"/>
		<geom conaffinity="0" fromto="-.3 -.3 .01 -.3 .3 .01" name="sideW" rgba="0.9 0.4 0.6 1" size=".02" type="capsule"/>
		<!-- Arm -->
		<geom conaffinity="0" contype="0" fromto="0 0 0 0 0 0.02" name="root" rgba="0.9 0.4 0.6 1" size=".011" type="cylinder"/>
		<body name="body0" pos="0 0 .01">
			<geom fromto="0 0 0 0.1 0 0" name="link0" rgba="0.0 0.4 0.6 1" size=".01" type="capsule"/>
			<joint axis="0 0 1" limited="false" name="joint0" pos="0 0 0" type="hinge"/>
			<body name="body1" pos="0.1 0 0">
				<joint axis="0 0 1" limited="true" name="joint1" pos="0 0 0" range="-3.0 3.0" type="hinge"/>
				<geom fromto="0 0 0 0.1 0 0" name="link1" rgba="0.0 0.4 0.6 1" size=".01" type="capsule"/>
				<body name="fingertip" pos="0.11 0 0">
					<geom contype="0" name="fingertip" pos="0 0 0" rgba="0.0 0.8 0.6 1" size=".01" type="sphere"/>
				</body>
			</body>
		</body>
		<!-- Target -->
		<body name="target" pos=".1 -.1 .01">
			<joint armature="0" axis="1 0 0" damping="0" limited="true" name="target_x" pos="0 0 0" range="-.27 .27" ref=".1" stiffness="0" type="slide"/>
			<joint armature="0" axis="0 1 0" damping="0" limited="true" name="target_y" pos="0 0 0" range="-.27 .27" ref="-.1" stiffness="0" type="slide"/>
			<geom conaffinity="0" contype="0" name="target" pos="0 0 0" rgba="0.9 0.2 0.2 1" size=".009" type="sphere"/>
		</body>
	</worldbody>
	<actuator>
		<motor ctrllimited="true" ctrlrange="-1.0 1.0" gear="200.0" joint="joint0"/>
		<motor ctrllimited="true" ctrlrange="-1.0 1.0" gear="200.0" joint="joint1"/>
	</actuator>
</mujoco>