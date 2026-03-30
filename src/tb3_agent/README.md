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
and opens the house navigation RViz debug view. It launches:
- a namespaced robot stack under `/robot_1`
- the fleet scheduler agent in the global namespace

## Operator task topic

The current task agent subscribes to `std_msgs/msg/String` on `/robot_1/task_request`.
Write natural-language requests there and the Bedrock planner converts them
into safe tool calls and structured JSON commands on `/robot_1/agent_command`.

Examples:

```bash
ros2 topic pub --once /robot_1/task_request std_msgs/msg/String '{data: "go to north west room"}'
```

```bash
ros2 topic pub --once /robot_1/task_request std_msgs/msg/String '{data: "navigate to x 5.8 y 0.0 yaw 0.0"}'
```

```bash
ros2 topic pub --once /robot_1/task_request std_msgs/msg/String '{data: "cancel"}'
```

```bash
ros2 topic pub --once /robot_1/task_request std_msgs/msg/String '{data: "what is the robot doing?"}'
```

```bash
ros2 topic pub --once /robot_1/task_request std_msgs/msg/String '{data: "where can you go?"}'
```

## Scheduler mission topic

The fleet scheduler subscribes to `std_msgs/msg/String` on `/scheduler/mission_requests`.
Use that for high-level missions. The scheduler now accepts work only from this
topic. It no longer loads missions from any YAML queue file.

The scheduler uses Bedrock at runtime to break each mission into resumable
subtasks and then delegates those subtasks to the current robot task agent.

By default, the scheduler now starts idle:
- it does not auto-resume unfinished missions from `.tb3_scheduler_state.json`

If you want a completely clean slate for testing, stop the launch and remove:

```bash
rm /home/vidushi/ros2_ws/.tb3_scheduler_state.json
```

Example:

```bash
ros2 topic pub --once /scheduler/mission_requests std_msgs/msg/String '{data: "Inspect the north side and report when done."}'
```

For urgent work, send a JSON envelope on the same topic:

```bash
ros2 topic pub --once /scheduler/mission_requests std_msgs/msg/String \
'{data: "{\"request_text\":\"Go to the hallway immediately and check the area.\",\"priority\":\"urgent\"}"}'
```

Urgent missions preempt the robot's current mission, run first, and then allow
the paused mission to resume afterward.

Control-style commands also go through the same topic:

```bash
ros2 topic pub --once /scheduler/mission_requests std_msgs/msg/String \
'{data: "stop all tasks and return to spawn"}'
```

That interrupts active work immediately instead of queueing a new mission.

The scheduler keeps its persisted mission and pause/resume state in:

```bash
/home/vidushi/ros2_ws/.tb3_scheduler_state.json
```

## Internal coordination topics

The scheduler sends structured task envelopes to:
- `/robot_1/scheduled_task_request`

The current task agent reports task outcomes on:
- `/robot_1/scheduled_task_result`

The executor still subscribes to:
- `/robot_1/agent_command`

Payloads on `/robot_1/agent_command` are JSON strings.

Examples:

```bash
ros2 topic pub --once /robot_1/agent_command std_msgs/msg/String '{data: "{\"action\":\"navigate\",\"target\":\"spawn\"}"}'
```

```bash
ros2 topic pub --once /robot_1/agent_command std_msgs/msg/String '{data: "{\"action\":\"navigate\",\"x\":0.0,\"y\":0.0,\"yaw\":0.0}"}'
```

```bash
ros2 topic pub --once /robot_1/agent_command std_msgs/msg/String '{data: "{\"action\":\"cancel\"}"}'
```

## Debug topic

The executor publishes the currently commanded goal marker to
`/robot_1/agent_goal_marker` as `visualization_msgs/msg/Marker` for RViz.

The task agent publishes Bedrock feedback to `/robot_1/agent_feedback`
as `std_msgs/msg/String`.

The executor publishes its latest navigation state on `/robot_1/agent_status`
as `std_msgs/msg/String` for the planner.

The scheduler publishes:
- `/scheduler/status`
- `/scheduler/feedback`
- `/scheduler/mission_plan`
- `/scheduler/active_assignments`
