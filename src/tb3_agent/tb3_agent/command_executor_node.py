import json
import math
import os

from ament_index_python.packages import get_package_share_directory
from geometry_msgs.msg import PoseStamped
from nav2_msgs.action import NavigateToPose
import rclpy
from rclpy.action import ActionClient
from rclpy.node import Node
from std_msgs.msg import String
from visualization_msgs.msg import Marker
import yaml


def yaw_to_quaternion(yaw: float) -> tuple[float, float, float, float]:
    half_yaw = yaw / 2.0
    return (0.0, 0.0, math.sin(half_yaw), math.cos(half_yaw))


class CommandExecutorNode(Node):
    def __init__(self) -> None:
        super().__init__('command_executor_node')

        locations_file = os.path.join(get_package_share_directory('tb3_agent'), 'config', 'locations.yaml')
        self.locations = self._load_locations(locations_file)
        self.nav_client = ActionClient(self, NavigateToPose, '/navigate_to_pose')
        self.goal_marker_publisher = self.create_publisher(Marker, '/agent_goal_marker', 10)
        self.pending_goal = None
        self.goal_handle = None
        self.goal_in_progress = False

        self.create_subscription(String, '/agent_command', self._command_callback, 10)
        self.create_timer(1.0, self._try_send_pending_goal)
        self.get_logger().info('Listening for JSON commands on /agent_command')

    def _load_locations(self, locations_file: str) -> dict:
        with open(locations_file, 'r', encoding='utf-8') as handle:
            data = yaml.safe_load(handle) or {}
        return data

    def _command_callback(self, msg: String) -> None:
        try:
            command = json.loads(msg.data)
        except json.JSONDecodeError as exc:
            self.get_logger().warning(f'Invalid JSON command: {exc}')
            return

        action = command.get('action')
        if action == 'navigate':
            self._handle_navigate(command)
            return
        if action == 'cancel':
            self._handle_cancel()
            return
        if action == 'status':
            self._handle_status()
            return

        self.get_logger().warning(f'Unknown action: {action}')

    def _handle_navigate(self, command: dict) -> None:
        if 'target' in command:
            location = self.locations.get(command['target'])
            if location is None:
                self.get_logger().warning(f'Unknown target: {command["target"]}')
                return
            x = float(location['x'])
            y = float(location['y'])
            yaw = float(location.get('yaw', 0.0))
        elif 'x' in command and 'y' in command:
            x = float(command['x'])
            y = float(command['y'])
            yaw = float(command.get('yaw', 0.0))
        else:
            self.get_logger().warning('Navigate command requires either target or x/y coordinates.')
            return

        pose = PoseStamped()
        pose.header.frame_id = 'map'
        pose.header.stamp = self.get_clock().now().to_msg()
        pose.pose.position.x = x
        pose.pose.position.y = y
        qx, qy, qz, qw = yaw_to_quaternion(yaw)
        pose.pose.orientation.x = qx
        pose.pose.orientation.y = qy
        pose.pose.orientation.z = qz
        pose.pose.orientation.w = qw

        self.pending_goal = pose
        self._publish_goal_marker(pose)
        self.get_logger().info(f'Queued navigation goal x={x:.2f}, y={y:.2f}, yaw={yaw:.2f}')
        self._try_send_pending_goal()

    def _handle_cancel(self) -> None:
        self.pending_goal = None
        self._delete_goal_marker()
        if self.goal_handle is None:
            self.get_logger().info('No active goal to cancel.')
            return
        cancel_future = self.goal_handle.cancel_goal_async()
        cancel_future.add_done_callback(self._cancel_done_callback)
        self.get_logger().info('Requested goal cancellation.')

    def _handle_status(self) -> None:
        if self.goal_in_progress:
            self.get_logger().info('Navigation status: goal active.')
            return
        if self.pending_goal is not None:
            self.get_logger().info('Navigation status: goal queued, waiting for Nav2.')
            return
        self.get_logger().info('Navigation status: idle.')

    def _try_send_pending_goal(self) -> None:
        if self.pending_goal is None or self.goal_in_progress:
            return
        if not self.nav_client.wait_for_server(timeout_sec=0.1):
            self.get_logger().info('Nav2 action server not ready yet')
            return

        goal = NavigateToPose.Goal()
        goal.pose = self.pending_goal
        self.goal_in_progress = True
        send_goal_future = self.nav_client.send_goal_async(goal)
        send_goal_future.add_done_callback(self._goal_response_callback)
        self.get_logger().info('Goal sent to Nav2')

    def _goal_response_callback(self, future) -> None:
        try:
            goal_handle = future.result()
        except Exception as exc:
            self.goal_in_progress = False
            self.get_logger().warning(f'Failed to send goal: {exc}')
            return

        if goal_handle is None or not goal_handle.accepted:
            self.goal_in_progress = False
            self.get_logger().warning('Goal rejected')
            return

        self.goal_handle = goal_handle
        self.pending_goal = None
        result_future = goal_handle.get_result_async()
        result_future.add_done_callback(self._goal_result_callback)

    def _goal_result_callback(self, future) -> None:
        self.goal_in_progress = False
        self.goal_handle = None
        try:
            result = future.result()
            self.get_logger().info(f'Navigation finished with status code {result.status}')
        except Exception as exc:
            self.get_logger().warning(f'Navigation failed: {exc}')

    def _cancel_done_callback(self, future) -> None:
        try:
            future.result()
            self.get_logger().info('Cancel request completed.')
        except Exception as exc:
            self.get_logger().warning(f'Cancel request failed: {exc}')
        finally:
            self.goal_in_progress = False
            self.goal_handle = None

    def _publish_goal_marker(self, pose: PoseStamped) -> None:
        marker = Marker()
        marker.header.frame_id = 'map'
        marker.header.stamp = self.get_clock().now().to_msg()
        marker.ns = 'agent_goal'
        marker.id = 0
        marker.type = Marker.ARROW
        marker.action = Marker.ADD
        marker.pose = pose.pose
        marker.scale.x = 0.5
        marker.scale.y = 0.1
        marker.scale.z = 0.1
        marker.color.a = 1.0
        marker.color.r = 0.1
        marker.color.g = 0.9
        marker.color.b = 0.2
        self.goal_marker_publisher.publish(marker)

    def _delete_goal_marker(self) -> None:
        marker = Marker()
        marker.header.frame_id = 'map'
        marker.header.stamp = self.get_clock().now().to_msg()
        marker.ns = 'agent_goal'
        marker.id = 0
        marker.action = Marker.DELETE
        self.goal_marker_publisher.publish(marker)


def main() -> None:
    rclpy.init()
    node = CommandExecutorNode()
    rclpy.spin(node)
    node.destroy_node()
    rclpy.shutdown()
