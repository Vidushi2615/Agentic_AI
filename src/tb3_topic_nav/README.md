# tb3_topic_nav

ROS 2 package with one node that subscribes to `/goal_pose` and forwards the pose to Nav2.

## Build

```bash
cd /home/vidushi/ros2_ws
source /opt/ros/humble/setup.bash
colcon build --packages-select tb3_topic_nav
source install/setup.bash
```

## Run

```bash
export TURTLEBOT3_MODEL=burger
export ROS_LOG_DIR=/home/vidushi/ros2_ws/log/ros
ros2 launch tb3_topic_nav sim_nav_bringup.launch.py
```

## Goal topic

The launch file starts Gazebo, Nav2, and `goal_pose_navigator`.
It also publishes `/initialpose` once for AMCL.
It uses the package-local Nav2 parameter file `config/nav2_burger.yaml`.

The node subscribes to `geometry_msgs/msg/PoseStamped` on `/goal_pose`.

Publish a goal with:

```bash
ros2 topic pub --once /goal_pose geometry_msgs/msg/PoseStamped \
'{
  header: {frame_id: "map"},
  pose: {
    position: {x: -1.0, y: 0.5, z: 0.0},
    orientation: {x: 0.0, y: 0.0, z: 0.7071, w: 0.7071}
  }
}'
```

Each received pose is sent to the `navigate_to_pose` action server.
