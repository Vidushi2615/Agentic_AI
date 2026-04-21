import os

from ament_index_python.packages import get_package_share_directory
from launch import LaunchDescription
from launch.actions import GroupAction, IncludeLaunchDescription
from launch.launch_description_sources import PythonLaunchDescriptionSource
from launch_ros.actions import PushRosNamespace
from launch_ros.actions import Node


def generate_launch_description() -> LaunchDescription:
    bringup = IncludeLaunchDescription(
        PythonLaunchDescriptionSource(
            os.path.join(
                get_package_share_directory('tb3_bringup'),
                'launch',
                'house_nav_bringup.launch.py',
            )
        )
    )

    robot_agent_stack = GroupAction(
        actions=[
            PushRosNamespace('robot_1'),
            Node(
                package='tb3_agent',
                executable='command_executor_node',
                name='command_executor_node',
                output='screen',
                parameters=[{'use_sim_time': True}],
            ),
            Node(
                package='tb3_agent',
                executable='task_agent_node',
                name='task_agent_node',
                output='screen',
                parameters=[{'use_sim_time': True}],
            ),
        ]
    )

    scheduler = Node(
        package='tb3_agent',
        executable='scheduler_agent_node',
        name='scheduler_agent_node',
        output='screen',
        parameters=[{'use_sim_time': True}],
    )

    scheduler_ui = Node(
        package='tb3_agent',
        executable='scheduler_ui_node',
        name='scheduler_ui_node',
        output='screen',
        parameters=[
            {
                'use_sim_time': True,
                'host': '127.0.0.1',
                'port': 8080,
            }
        ],
    )

    return LaunchDescription([bringup, robot_agent_stack, scheduler, scheduler_ui])
