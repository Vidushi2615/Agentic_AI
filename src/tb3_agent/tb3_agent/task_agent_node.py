import json
import os
import re

from ament_index_python.packages import get_package_share_directory
import rclpy
from rclpy.node import Node
from std_msgs.msg import String
import yaml


class TaskAgentNode(Node):
    def __init__(self) -> None:
        super().__init__('task_agent_node')

        locations_file = os.path.join(get_package_share_directory('tb3_agent'), 'config', 'locations.yaml')
        self.locations = self._load_locations(locations_file)
        self.location_aliases = self._build_location_aliases(self.locations)
        self.command_publisher = self.create_publisher(String, '/agent_command', 10)
        self.feedback_publisher = self.create_publisher(String, '/agent_feedback', 10)

        self.create_subscription(String, '/task_request', self._task_callback, 10)
        self.get_logger().info('Listening for natural-language tasks on /task_request')

    def _load_locations(self, locations_file: str) -> dict:
        with open(locations_file, 'r', encoding='utf-8') as handle:
            data = yaml.safe_load(handle) or {}
        return data

    def _build_location_aliases(self, locations: dict) -> dict:
        aliases = {}
        for name in locations:
            alias_set = {
                name,
                name.replace('_', ' '),
                name.replace('_', ''),
                name.replace('_', '-'),
            }
            for alias in alias_set:
                aliases[alias.lower()] = name
        return aliases

    def _task_callback(self, msg: String) -> None:
        raw_task = msg.data.strip()
        if not raw_task:
            self._publish_feedback('Ignored empty task request.')
            return

        normalized = re.sub(r'\s+', ' ', raw_task.strip().lower())
        if normalized in {
            'list locations',
            'show locations',
            'where can you go',
            'what locations do you know',
        }:
            self._publish_feedback('Available locations: ' + ', '.join(sorted(self.locations.keys())))
            return

        command = self._parse_task(raw_task)
        if command is None:
            self._publish_feedback(f'Could not understand task: {raw_task}')
            return

        command_msg = String()
        command_msg.data = json.dumps(command)
        self.command_publisher.publish(command_msg)
        self._publish_feedback(f'Published command: {command_msg.data}')

    def _parse_task(self, task_text: str) -> dict | None:
        normalized = re.sub(r'\s+', ' ', task_text.strip().lower())

        if normalized in {'cancel', 'stop', 'abort', 'cancel navigation', 'stop navigation'}:
            return {'action': 'cancel'}

        if normalized in {'status', 'what is the status', 'report status'}:
            return {'action': 'status'}

        coordinate_command = self._parse_coordinate_command(normalized)
        if coordinate_command is not None:
            return coordinate_command

        target_name = self._match_location(normalized)
        if target_name is not None and any(
            phrase in normalized
            for phrase in ('go to', 'navigate to', 'move to', 'head to', 'travel to', 'take the robot to')
        ):
            return {'action': 'navigate', 'target': target_name}

        return None

    def _parse_coordinate_command(self, normalized: str) -> dict | None:
        xy_yaw_match = re.search(
            r'(?:go to|navigate to|move to|head to|travel to)\s+'
            r'x\s*(-?\d+(?:\.\d+)?)\s+'
            r'y\s*(-?\d+(?:\.\d+)?)'
            r'(?:\s+yaw\s*(-?\d+(?:\.\d+)?))?',
            normalized,
        )
        if xy_yaw_match is not None:
            command = {
                'action': 'navigate',
                'x': float(xy_yaw_match.group(1)),
                'y': float(xy_yaw_match.group(2)),
            }
            if xy_yaw_match.group(3) is not None:
                command['yaw'] = float(xy_yaw_match.group(3))
            return command

        bare_match = re.search(
            r'(?:go to|navigate to|move to|head to|travel to)\s+'
            r'(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)(?:\s+(-?\d+(?:\.\d+)?))?',
            normalized,
        )
        if bare_match is not None:
            command = {
                'action': 'navigate',
                'x': float(bare_match.group(1)),
                'y': float(bare_match.group(2)),
            }
            if bare_match.group(3) is not None:
                command['yaw'] = float(bare_match.group(3))
            return command

        return None

    def _match_location(self, normalized: str) -> str | None:
        matches = []
        for alias, canonical_name in self.location_aliases.items():
            if alias in normalized:
                matches.append((len(alias), canonical_name))

        if not matches:
            return None

        matches.sort(reverse=True)
        return matches[0][1]

    def _publish_feedback(self, message: str) -> None:
        feedback_msg = String()
        feedback_msg.data = message
        self.feedback_publisher.publish(feedback_msg)
        self.get_logger().info(message)


def main() -> None:
    rclpy.init()
    node = TaskAgentNode()
    rclpy.spin(node)
    node.destroy_node()
    rclpy.shutdown()
