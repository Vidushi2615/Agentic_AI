import os

from ament_index_python.packages import get_package_share_directory
from launch import LaunchDescription
from launch.actions import IncludeLaunchDescription
from launch.launch_description_sources import PythonLaunchDescriptionSource
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

    executor = Node(
        package='tb3_agent',
        executable='command_executor_node',
        name='command_executor_node',
        output='screen',
        parameters=[{'use_sim_time': True}],
    )

    return LaunchDescription([bringup, executor])
