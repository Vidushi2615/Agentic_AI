import json
import os

from ament_index_python.packages import get_package_share_directory
import rclpy
from rclpy.node import Node
from std_msgs.msg import String
import yaml

from tb3_agent.bedrock_planner import BedrockPlanner, BedrockPlannerError


STATUS_UNKNOWN = 'unknown'
EXECUTION_TOOLS = {'navigate_named_location', 'navigate_coordinates', 'cancel_navigation'}
TERMINAL_FAILURE_PREFIXES = {
    'navigation finished with status code',
    'navigation failed',
    'goal rejected',
    'failed to send goal',
    'cancel request failed',
}
SUCCESS_STATUS = 'navigation finished with status code 4'
CANCELLED_STATUS = 'navigation finished with status code 5'


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
        self.command_publisher = self.create_publisher(String, 'agent_command', 10)
        self.feedback_publisher = self.create_publisher(String, 'agent_feedback', 10)
        self.scheduled_result_publisher = self.create_publisher(String, 'scheduled_task_result', 10)
        self.planner = BedrockPlanner(
            system_prompt=self._load_system_prompt(prompt_file),
            tools=self._build_tool_config(),
            env_file=env_file,
        )
        self._config_warning_sent = False
        self.active_scheduled_task = None
        self.awaiting_scheduled_execution_result = False
        self.awaiting_scheduled_execution_mode = None

        self.create_subscription(String, 'task_request', self._task_callback, 10)
        self.create_subscription(String, 'scheduled_task_request', self._scheduled_task_callback, 10)
        self.create_subscription(String, 'agent_status', self._status_callback, 10)
        self.create_timer(2.0, self._publish_configuration_warning)
        self.get_logger().info('Listening for Bedrock-planned tasks on task_request')

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

        self._run_task(raw_task, extra_context='')

    def _scheduled_task_callback(self, msg: String) -> None:
        try:
            envelope = json.loads(msg.data)
        except json.JSONDecodeError as exc:
            self._publish_feedback(f'Invalid scheduled task envelope: {exc}')
            return

        dispatch_reason = str(envelope.get('dispatch_reason', '')).strip()
        if (
            self.active_scheduled_task is not None
            and self.awaiting_scheduled_execution_result
            and dispatch_reason not in {'stop_work', 'return_home'}
        ):
            message = (
                'Robot task agent is already waiting for the current scheduled task to finish; '
                'rejected overlapping scheduled task.'
            )
            self._publish_feedback(message)
            self._publish_scheduled_result_for_envelope(envelope, 'blocked', message)
            return

        self.active_scheduled_task = envelope
        self.awaiting_scheduled_execution_result = False
        self.awaiting_scheduled_execution_mode = None
        if not envelope.get('instruction_text', '').strip():
            self._publish_feedback('Scheduled task envelope is missing instruction_text.')
            self._publish_scheduled_result('failed', 'Scheduled task envelope is missing instruction_text.')
            return
        extra_context = (
            f"Mission id: {envelope.get('mission_id', '')}\n"
            f"Subtask id: {envelope.get('subtask_id', '')}\n"
            f"Dispatch reason: {envelope.get('dispatch_reason', '')}\n"
            f"Resume context: {envelope.get('resume_context', '')}\n"
            f"Completion criteria: {json.dumps(envelope.get('completion_criteria', []))}\n"
            f"Expected outcome tags: {json.dumps(envelope.get('outcome_tags', []))}"
        )
        self._run_task(envelope.get('instruction_text', '').strip(), extra_context=extra_context)

    def _run_task(self, task_text: str, *, extra_context: str) -> None:
        if not task_text:
            self._publish_feedback('Ignored empty task.')
            return

        try:
            response_text, executed_tools = self.planner.run_task(
                task_text,
                dynamic_context=self._build_dynamic_context(extra_context),
                tool_handler=self._handle_tool_call,
            )
        except BedrockPlannerError as exc:
            self._publish_feedback(str(exc))
            self._publish_scheduled_result('failed', str(exc))
            return

        execution_tool_used = self._has_execution_tool(executed_tools)

        if self.active_scheduled_task is not None:
            if execution_tool_used:
                self.awaiting_scheduled_execution_result = True
                self.awaiting_scheduled_execution_mode = self._scheduled_execution_mode(executed_tools)
                task_id = self.active_scheduled_task.get('subtask_id', '')
                self._publish_feedback(
                    f'Scheduled task {task_id} was sent to the executor. Waiting for navigation result.'
                )
                return

            if self._status_indicates_navigation_active(self.current_status):
                self.awaiting_scheduled_execution_result = True
                self.awaiting_scheduled_execution_mode = 'navigation'
                task_id = self.active_scheduled_task.get('subtask_id', '')
                self._publish_feedback(
                    f'Scheduled task {task_id} is associated with active navigation. Waiting for navigation result.'
                )
                return

            message = response_text or 'The task agent could not identify an executable next step.'
            self._publish_feedback(message)
            self._publish_scheduled_result('needs_clarification', message)
            return

        if response_text:
            self._publish_feedback(response_text)

    def _status_callback(self, msg: String) -> None:
        self.current_status = msg.data.strip() or STATUS_UNKNOWN
        if self.active_scheduled_task is None or not self.awaiting_scheduled_execution_result:
            return
        if self.current_status == SUCCESS_STATUS:
            self._publish_scheduled_result('completed', self.current_status)
        elif self.current_status == CANCELLED_STATUS:
            self._publish_scheduled_result('cancelled', self.current_status)
        elif self.awaiting_scheduled_execution_mode == 'cancel_only' and self.current_status == 'idle':
            self._publish_scheduled_result('cancelled', 'Cancellation completed and robot is idle.')
        elif self._is_terminal_failure_status(self.current_status):
            self._publish_scheduled_result('failed', self.current_status)

    def _publish_configuration_warning(self) -> None:
        if self._config_warning_sent:
            return
        issues = self.planner.validate_configuration()
        if issues:
            self._publish_feedback(' '.join(issues))
        self._config_warning_sent = True

    def _build_dynamic_context(self, extra_context: str = '') -> str:
        location_lines = []
        for name, location in sorted(self.locations.items()):
            location_lines.append(
                f'- {name}: x={location["x"]}, y={location["y"]}, yaw={location.get("yaw", 0.0)}'
            )
        base_context = (
            'Robot capabilities:\n'
            '- Navigate to named locations.\n'
            '- Navigate to map coordinates.\n'
            '- Cancel navigation.\n'
            '- Report navigation status.\n\n'
            f'Current navigation status: {self.current_status}\n\n'
            'Available locations:\n'
            + '\n'.join(location_lines)
        )
        if extra_context:
            base_context += f'\n\nScheduler context:\n{extra_context}'
        return base_context

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

    def _publish_scheduled_result(self, status: str, message: str) -> None:
        if self.active_scheduled_task is None:
            return
        self._publish_scheduled_result_for_envelope(self.active_scheduled_task, status, message)
        if status in {'completed', 'failed', 'cancelled', 'needs_clarification'}:
            self.active_scheduled_task = None
            self.awaiting_scheduled_execution_result = False
            self.awaiting_scheduled_execution_mode = None

    def _publish_scheduled_result_for_envelope(self, envelope: dict, status: str, message: str) -> None:
        result = {
            'mission_id': envelope.get('mission_id', ''),
            'subtask_id': envelope.get('subtask_id', ''),
            'robot_id': envelope.get('robot_id', self.get_namespace().strip('/')),
            'status': status,
            'message': message,
            'completed_outcome_tags': self._completed_outcome_tags(status, envelope),
            'completion_summary': self._completion_summary(status, message, envelope),
        }
        self.scheduled_result_publisher.publish(String(data=json.dumps(result)))

    @staticmethod
    def _has_execution_tool(executed_tools: list[dict]) -> bool:
        for tool in executed_tools:
            if tool.get('name') in EXECUTION_TOOLS:
                return True
        return False

    @staticmethod
    def _is_terminal_failure_status(status: str) -> bool:
        for prefix in TERMINAL_FAILURE_PREFIXES:
            if status.startswith(prefix):
                return True
        return False

    @staticmethod
    def _scheduled_execution_mode(executed_tools: list[dict]) -> str:
        tool_names = {tool.get('name') for tool in executed_tools}
        if tool_names == {'cancel_navigation'}:
            return 'cancel_only'
        return 'navigation'

    @staticmethod
    def _status_indicates_navigation_active(status: str) -> bool:
        normalized = status.strip().lower()
        return normalized in {
            'goal accepted',
            'goal sent to nav2',
            'goal active',
        } or normalized.startswith('queued ')

    @staticmethod
    def _completed_outcome_tags(status: str, envelope: dict) -> list[str]:
        if status != 'completed':
            return []
        tags = envelope.get('outcome_tags', [])
        if not isinstance(tags, list):
            return []
        return [str(tag).strip() for tag in tags if str(tag).strip()]

    @staticmethod
    def _completion_summary(status: str, message: str, envelope: dict) -> str:
        instruction = envelope.get('instruction_text', '')
        if status == 'completed':
            return f'Completed scheduled instruction: {instruction}. Executor result: {message}'
        return f'Scheduled instruction did not complete: {instruction}. Executor result: {message}'

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
