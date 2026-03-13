import json
import os

from ament_index_python.packages import get_package_share_directory
import rclpy
from rclpy.node import Node
from std_msgs.msg import String
import yaml

from tb3_agent.bedrock_planner import BedrockPlanner, BedrockPlannerError


STATUS_UNKNOWN = 'unknown'


class TaskAgentNode(Node):
    def __init__(self) -> None:
        super().__init__('task_agent_node')

        package_share = get_package_share_directory('tb3_agent')
        locations_file = os.path.join(package_share, 'config', 'locations.yaml')
        prompt_file = os.path.join(package_share, 'config', 'agent_system_prompt.txt')
        workspace_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(package_share))))
        env_file = os.path.join(workspace_root, '.env')

        self.locations = self._load_locations(locations_file)
        self.current_status = STATUS_UNKNOWN
        self.command_publisher = self.create_publisher(String, '/agent_command', 10)
        self.feedback_publisher = self.create_publisher(String, '/agent_feedback', 10)
        self.planner = BedrockPlanner(
            system_prompt=self._load_system_prompt(prompt_file),
            tools=self._build_tool_config(),
            env_file=env_file,
        )
        self._config_warning_sent = False

        self.create_subscription(String, '/task_request', self._task_callback, 10)
        self.create_subscription(String, '/agent_status', self._status_callback, 10)
        self.create_timer(2.0, self._publish_configuration_warning)
        self.get_logger().info('Listening for Bedrock-planned tasks on /task_request')

    def _load_locations(self, locations_file: str) -> dict:
        with open(locations_file, 'r', encoding='utf-8') as handle:
            data = yaml.safe_load(handle) or {}
        return data

    def _load_system_prompt(self, prompt_file: str) -> str:
        with open(prompt_file, 'r', encoding='utf-8') as handle:
            return handle.read()

    def _task_callback(self, msg: String) -> None:
        raw_task = msg.data.strip()
        if not raw_task:
            self._publish_feedback('Ignored empty task request.')
            return

        try:
            response_text, _ = self.planner.run_task(
                raw_task,
                dynamic_context=self._build_dynamic_context(),
                tool_handler=self._handle_tool_call,
            )
        except BedrockPlannerError as exc:
            self._publish_feedback(str(exc))
            return

        if response_text:
            self._publish_feedback(response_text)

    def _status_callback(self, msg: String) -> None:
        self.current_status = msg.data.strip() or STATUS_UNKNOWN

    def _publish_configuration_warning(self) -> None:
        if self._config_warning_sent:
            return
        issues = self.planner.validate_configuration()
        if issues:
            self._publish_feedback(' '.join(issues))
        self._config_warning_sent = True

    def _build_dynamic_context(self) -> str:
        location_lines = []
        for name, location in sorted(self.locations.items()):
            location_lines.append(
                f'- {name}: x={location["x"]}, y={location["y"]}, yaw={location.get("yaw", 0.0)}'
            )
        return (
            'Robot capabilities:\n'
            '- Navigate to named locations.\n'
            '- Navigate to map coordinates.\n'
            '- Cancel navigation.\n'
            '- Report navigation status.\n\n'
            f'Current navigation status: {self.current_status}\n\n'
            'Available locations:\n'
            + '\n'.join(location_lines)
        )

    def _handle_tool_call(self, tool_name: str, tool_input: dict) -> dict:
        if tool_name == 'navigate_named_location':
            return self._tool_navigate_named_location(tool_input)
        if tool_name == 'navigate_coordinates':
            return self._tool_navigate_coordinates(tool_input)
        if tool_name == 'cancel_navigation':
            return self._tool_cancel_navigation()
        if tool_name == 'get_navigation_status':
            return self._tool_get_navigation_status()
        if tool_name == 'list_locations':
            return self._tool_list_locations()
        return {'ok': False, 'error': f'Unknown tool: {tool_name}'}

    def _tool_navigate_named_location(self, tool_input: dict) -> dict:
        location_name = str(tool_input.get('location_name', '')).strip()
        location = self.locations.get(location_name)
        if location is None:
            return {
                'ok': False,
                'error': f'Unknown location: {location_name}',
                'available_locations': sorted(self.locations.keys()),
            }

        command = {'action': 'navigate', 'target': location_name}
        self._publish_command(command)
        return {'ok': True, 'command': command, 'resolved_location': location}

    def _tool_navigate_coordinates(self, tool_input: dict) -> dict:
        try:
            x = float(tool_input['x'])
            y = float(tool_input['y'])
            yaw = float(tool_input.get('yaw', 0.0))
        except (KeyError, TypeError, ValueError):
            return {'ok': False, 'error': 'x and y must be numeric. yaw is optional and must be numeric.'}

        command = {'action': 'navigate', 'x': x, 'y': y, 'yaw': yaw}
        self._publish_command(command)
        return {'ok': True, 'command': command}

    def _tool_cancel_navigation(self) -> dict:
        command = {'action': 'cancel'}
        self._publish_command(command)
        return {'ok': True, 'command': command}

    def _tool_get_navigation_status(self) -> dict:
        return {'ok': True, 'status': self.current_status}

    def _tool_list_locations(self) -> dict:
        return {'ok': True, 'locations': self.locations}

    def _publish_command(self, command: dict) -> None:
        command_msg = String()
        command_msg.data = json.dumps(command)
        self.command_publisher.publish(command_msg)

    def _publish_feedback(self, message: str) -> None:
        feedback_msg = String()
        feedback_msg.data = message
        self.feedback_publisher.publish(feedback_msg)
        self.get_logger().info(message)

    @staticmethod
    def _build_tool_config() -> list[dict]:
        return [
            {
                'toolSpec': {
                    'name': 'navigate_named_location',
                    'description': 'Navigate the robot to a known named location from the map.',
                    'inputSchema': {
                        'json': {
                            'type': 'object',
                            'properties': {'location_name': {'type': 'string'}},
                            'required': ['location_name'],
                        }
                    },
                }
            },
            {
                'toolSpec': {
                    'name': 'navigate_coordinates',
                    'description': 'Navigate the robot to explicit map coordinates.',
                    'inputSchema': {
                        'json': {
                            'type': 'object',
                            'properties': {
                                'x': {'type': 'number'},
                                'y': {'type': 'number'},
                                'yaw': {'type': 'number'},
                            },
                            'required': ['x', 'y'],
                        }
                    },
                }
            },
            {
                'toolSpec': {
                    'name': 'cancel_navigation',
                    'description': 'Cancel the current navigation goal.',
                    'inputSchema': {'json': {'type': 'object', 'properties': {}}},
                }
            },
            {
                'toolSpec': {
                    'name': 'get_navigation_status',
                    'description': 'Get the current navigation status of the robot.',
                    'inputSchema': {'json': {'type': 'object', 'properties': {}}},
                }
            },
            {
                'toolSpec': {
                    'name': 'list_locations',
                    'description': 'List the known named navigation locations.',
                    'inputSchema': {'json': {'type': 'object', 'properties': {}}},
                }
            },
        ]


def main() -> None:
    rclpy.init()
    node = TaskAgentNode()
    rclpy.spin(node)
    node.destroy_node()
    rclpy.shutdown()
