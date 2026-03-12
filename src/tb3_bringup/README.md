# tb3_bringup

ROS 2 package for bringing up the TurtleBot3 house world with Nav2.

## Build

```bash
cd /home/vidushi/ros2_ws
source /opt/ros/humble/setup.bash
colcon build --packages-select tb3_bringup
source install/setup.bash
```

## Run

```bash
export TURTLEBOT3_MODEL=burger
export ROS_LOG_DIR=/home/vidushi/ros2_ws/log/ros
ros2 launch tb3_bringup house_nav_bringup.launch.py
```

The launch file starts Gazebo, Nav2, RViz, and publishes `/initialpose` once for AMCL.
It uses the local house map at `/home/vidushi/ros2_ws/maps/map_house.yaml`
and the package-local Nav2 parameter file `config/nav2_burger.yaml`.

The RViz debug view shows:
- `/map`
- `/odom`
- `/scan`
- TF and RobotModel
- `/agent_goal_marker`
- Nav2 global/local path and costmaps
