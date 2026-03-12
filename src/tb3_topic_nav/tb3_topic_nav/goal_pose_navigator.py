from geometry_msgs.msg import PoseStamped
from nav2_msgs.action import NavigateToPose
import rclpy
from rclpy.action import ActionClient
from rclpy.node import Node


class GoalPoseNavigator(Node):
    def __init__(self):
        super().__init__('goal_pose_navigator')
        self.client = ActionClient(self, NavigateToPose, '/navigate_to_pose')
        self.pending_goal = None
        self.goal_in_progress = False
        self.create_subscription(PoseStamped, '/goal_pose', self.callback, 10)
        self.create_timer(1.0, self._try_send_pending_goal)

    def callback(self, msg):
        self.pending_goal = msg
        self.get_logger().info(
            f'Received goal x={msg.pose.position.x:.2f}, y={msg.pose.position.y:.2f}. Waiting for Nav2 if needed.'
        )
        self._try_send_pending_goal()

    def _try_send_pending_goal(self):
        if self.pending_goal is None or self.goal_in_progress:
            return
        if not self.client.wait_for_server(timeout_sec=0.1):
            self.get_logger().info('Nav2 action server not ready yet')
            return

        goal = NavigateToPose.Goal()
        goal.pose = self.pending_goal
        self.goal_in_progress = True
        send_goal_future = self.client.send_goal_async(goal)
        send_goal_future.add_done_callback(self._goal_response_callback)
        self.get_logger().info('Goal sent')

    def _goal_response_callback(self, future):
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

        self.pending_goal = None
        result_future = goal_handle.get_result_async()
        result_future.add_done_callback(self._goal_result_callback)

    def _goal_result_callback(self, future):
        self.goal_in_progress = False
        try:
            result = future.result()
            self.get_logger().info(f'Navigation finished with status code {result.status}')
        except Exception as exc:
            self.get_logger().warning(f'Navigation failed: {exc}')


def main():
    rclpy.init()
    node = GoalPoseNavigator()
    rclpy.spin(node)
    node.destroy_node()
    rclpy.shutdown()
