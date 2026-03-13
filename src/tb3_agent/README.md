# tb3_agent

ROS 2 package for agent-side command execution on TurtleBot3.

## Bedrock setup

Install `boto3` into the Python environment used by your ROS 2 workspace:

```bash
python3 -m pip install boto3
```

Export the Bedrock configuration before launching:

```bash
export AWS_BEARER_TOKEN_BEDROCK=your_bedrock_api_key
export BEDROCK_MODEL_ID=your_claude_sonnet_4_6_model_id
export AWS_DEFAULT_REGION=eu-west-2
```

You can also store them in a workspace-level `.env` file so you do not need
to export them in every terminal. The planner automatically reads
`/home/vidushi/ros2_ws/.env` if it exists.

Example:

```bash
cp /home/vidushi/ros2_ws/src/tb3_agent/config/bedrock.env.example /home/vidushi/ros2_ws/.env
```

Then edit `/home/vidushi/ros2_ws/.env`:

```bash
AWS_BEARER_TOKEN_BEDROCK=your_bedrock_api_key
BEDROCK_MODEL_ID=anthropic.claude-sonnet-4-6
AWS_DEFAULT_REGION=eu-west-2
```

## Run

```bash
export TURTLEBOT3_MODEL=burger
export ROS_LOG_DIR=/home/vidushi/ros2_ws/log/ros
ros2 launch tb3_agent agent_bringup.launch.py
```

This launch includes `tb3_bringup`, which starts `turtlebot3_house.world`
with the local map `/home/vidushi/ros2_ws/maps/map_house.yaml`
and opens the house navigation RViz debug view.

## Task topic

The task agent subscribes to `std_msgs/msg/String` on `/task_request`.
Write natural-language requests there and the Bedrock planner converts them
into safe tool calls and structured JSON commands on `/agent_command`.

Examples:

```bash
ros2 topic pub --once /task_request std_msgs/msg/String '{data: "go to north west room"}'
```

```bash
ros2 topic pub --once /task_request std_msgs/msg/String '{data: "navigate to x 5.8 y 0.0 yaw 0.0"}'
```

```bash
ros2 topic pub --once /task_request std_msgs/msg/String '{data: "cancel"}'
```

```bash
ros2 topic pub --once /task_request std_msgs/msg/String '{data: "what is the robot doing?"}'
```

```bash
ros2 topic pub --once /task_request std_msgs/msg/String '{data: "where can you go?"}'
```

## Command topic

The executor still subscribes to `std_msgs/msg/String` on `/agent_command`.
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

The task agent publishes Bedrock feedback to `/agent_feedback`
as `std_msgs/msg/String`.

The executor publishes its latest navigation state on `/agent_status`
as `std_msgs/msg/String` for the planner.
