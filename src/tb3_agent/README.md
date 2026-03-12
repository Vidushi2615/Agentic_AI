# tb3_agent

ROS 2 package for agent-side command execution on TurtleBot3.

## Run

```bash
export TURTLEBOT3_MODEL=burger
export ROS_LOG_DIR=/home/vidushi/ros2_ws/log/ros
ros2 launch tb3_agent agent_bringup.launch.py
```

This launch includes `tb3_bringup`, which starts `turtlebot3_house.world`
with the local map `/home/vidushi/ros2_ws/maps/map_house.yaml`
and opens the house navigation RViz debug view.

## Command topic

The executor subscribes to `std_msgs/msg/String` on `/agent_command`.
Payloads are JSON strings.

Examples:

```bash
ros2 topic pub --once /agent_command std_msgs/msg/String '{data: "{\"action\":\"navigate\",\"target\":\"spawn\"}"}'
```

```bash
ros2 topic pub --once /agent_command std_msgs/msg/String '{data: "{\"action\":\"navigate\",\"x\":0.0,\"y\":0.0,\"yaw\":0.0}"}'
```

```bash
ros2 topic pub --once /agent_command std_msgs/msg/String '{data: "{\"action\":\"cancel\"}"}'
```

## Debug topic

The executor publishes the currently commanded goal marker to
`/agent_goal_marker` as `visualization_msgs/msg/Marker` for RViz.
